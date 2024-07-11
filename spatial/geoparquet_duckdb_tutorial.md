# Python + GeoParquet + DuckDB: A Comprehensive Tutorial

## 1. Introduction

This tutorial explores the powerful combination of Python, GeoParquet, and DuckDB for efficient geospatial data processing and analysis. We'll cover the basics of each technology, their advantages, and how they work together to provide a robust solution for handling geospatial datasets.

### 1.1 What is GeoParquet?

GeoParquet is an extension of Apache Parquet, a columnar storage file format, designed specifically for geospatial data. It combines the efficiency of Parquet with support for geometric data types, making it an excellent choice for storing and processing geospatial information.

### 1.2 Advantages of GeoParquet

GeoParquet offers several advantages over alternative formats such as JSON, JSONL (JSON Lines), and CSV, especially when dealing with large geospatial datasets:

1. **Efficient Storage**: Uses columnar storage and compression, significantly reducing file size.
2. **Fast Query Performance**: Allows for quick data retrieval and filtering.
3. **Schema Enforcement**: Ensures data consistency and reduces interpretation errors.
4. **Support for Complex Data Types**: Natively stores complex geospatial objects.
5. **Partitioning and Chunking**: Supports efficient querying of subsets of large datasets.
6. **Interoperability**: Wide support in big data ecosystems and geospatial tools.
7. **Metadata Handling**: Better support for metadata compared to CSV.
8. **Streaming Capabilities**: Supports streaming reads with compression benefits.
9. **Reduced Processing Time**: Faster overall processing for large datasets.

### 1.3 Comparison with Alternative Formats

- **JSON Blobs**:
  - Pros: Human-readable, flexible schema
  - Cons: Large file size, slow to parse, must often be read entirely into memory

- **JSONL (JSON Lines)**:
  - Pros: Supports streaming, one record per line for easier processing
  - Cons: Still larger file size than GeoParquet, less efficient querying

- **CSV**:
  - Pros: Simple, widely supported, human-readable
  - Cons: No native support for complex data types, no schema enforcement, less efficient for large datasets

## 2. Setting Up the Environment

### 2.1 Installation

To set up our environment, we need to install the following packages:

```bash
pip install geopandas pyarrow duckdb pandas polars shapely
```

### 2.2 Importing Necessary Modules

In your Python script or Jupyter notebook, start with these imports:

```python
import geopandas as gpd
import pandas as pd
import polars as pl
import pyarrow as pa
import duckdb
import shapely
```

## 3. Working with GeoParquet and DuckDB

Let's create a simple example to demonstrate how to create, save, and read GeoParquet data using Python, GeoPandas, and DuckDB.

```python
import geopandas as gpd
import duckdb

# Print version information
print(f"GeoPandas version: {gpd.__version__}")
print(f"DuckDB version: {duckdb.__version__}")

# Create a simple GeoDataFrame
gdf = gpd.GeoDataFrame(
    {'city': ['New York', 'Paris', 'Tokyo'],
     'geometry': gpd.points_from_xy([-74.006, 2.3522, 139.6917], 
                                    [40.7128, 48.8566, 35.6895])},
    crs="EPSG:4326"
)

# Save as GeoParquet
gdf.to_parquet("cities.geoparquet")

# Read with DuckDB
con = duckdb.connect()

# Enable spatial extension
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# Read the GeoParquet file and extract coordinates
result = con.execute("""
    SELECT 
        city, 
        ST_X(ST_GeomFromWKB(geometry)) as longitude, 
        ST_Y(ST_GeomFromWKB(geometry)) as latitude
    FROM read_parquet('cities.geoparquet')
""").fetchall()

print("\nData read from GeoParquet using DuckDB:")
for row in result:
    print(f"City: {row[0]}, Longitude: {row[1]}, Latitude: {row[2]}")

con.close()
```

### 3.1 Understanding ST_GeomFromWKB

In our DuckDB query, we use the `ST_GeomFromWKB` function. Here's why it's necessary:

1. **WKB Format**: GeoParquet stores geometry data in Well-Known Binary (WKB) format. This is a standard binary representation of geometry data that's compact and efficient.

2. **DuckDB Interpretation**: While DuckDB can read the Parquet file, it doesn't automatically recognize the WKB data as geometry. The `ST_GeomFromWKB` function tells DuckDB to interpret this binary data as geometric information.

3. **Enabling Spatial Functions**: By converting the WKB data to a geometry type that DuckDB understands, we can then use spatial functions like `ST_X` and `ST_Y` to extract coordinates.

## 4. Processing GeoParquet with Different Tools

### 4.1 Using Pandas and GeoPandas

Pandas can read Parquet files directly, but it doesn't natively understand the geometry column. We'll need to use GeoPandas to properly interpret the geometry data and perform accurate spatial operations.

```python
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Read the GeoParquet file
pdf = pd.read_parquet('cities.geoparquet')
print("Pandas DataFrame:")
print(pdf)

# Convert to GeoDataFrame to properly handle the geometry
gdf = gpd.read_parquet('cities.geoparquet')
print("\nGeoPandas GeoDataFrame:")
print(gdf)

# Basic querying
print("\nCities with longitude < 0:")
print(gdf[gdf.geometry.x < 0])

# Calculate distances between cities
# First, we need to project our data to a coordinate system that preserves distance
# We'll use the World Equidistant Cylindrical projection (EPSG:4087)
gdf_projected = gdf.to_crs(epsg=4087)
tokyo_point = Point(139.6917, 35.6895)
tokyo_projected = gpd.GeoDataFrame(geometry=[tokyo_point], crs="EPSG:4326").to_crs(epsg=4087)

gdf_projected['distance_to_tokyo'] = gdf_projected.geometry.distance(tokyo_projected.geometry.iloc[0])

# Convert distance to kilometers
gdf_projected['distance_to_tokyo_km'] = gdf_projected['distance_to_tokyo'] / 1000

print("\nDistances to Tokyo:")
print(gdf_projected[['city', 'distance_to_tokyo_km']])
```

### 4.2 Using Polars

Polars is a fast dataframe library written in Rust. It can read Parquet files efficiently, but like pandas, it doesn't natively understand the geometry column. We'll need to handle the WKB data explicitly and implement our own distance calculation.

```python
import polars as pl
from shapely import wkb
import pyarrow as pa
import math

# Read the GeoParquet file
df = pl.read_parquet('cities.geoparquet')
print("Polars DataFrame:")
print(df)

# Function to convert WKB to coordinates
def wkb_to_coords(wkb_data):
    point = wkb.loads(wkb_data)
    return (point.x, point.y)

# Haversine formula for distance calculation
def haversine_distance(lon1, lat1, lon2, lat2):
    R = 6371  # Earth's radius in kilometers

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

# Extract coordinates from the geometry column
df_with_coords = df.with_columns([
    pl.col('geometry').map_elements(wkb_to_coords).alias('coords')
])
df_with_coords = df_with_coords.with_columns([
    pl.col('coords').list.get(0).alias('longitude'),
    pl.col('coords').list.get(1).alias('latitude')
])

print("\nPolars DataFrame with extracted coordinates:")
print(df_with_coords)

# Basic querying
print("\nCities with longitude < 0:")
print(df_with_coords.filter(pl.col('longitude') < 0))

# Calculate distances using Haversine formula
tokyo_coords = (139.6917, 35.6895)
df_with_distances = df_with_coords.with_columns([
    pl.struct(['longitude', 'latitude'])
    .map_elements(lambda x: haversine_distance(x['longitude'], x['latitude'], tokyo_coords[0], tokyo_coords[1]))
    .alias('distance_to_tokyo_km')
])

print("\nDistances to Tokyo (in kilometers):")
print(df_with_distances.select(['city', 'distance_to_tokyo_km']))
```

### 4.3 Using DuckDB

Here's an expanded example using DuckDB, which includes distance calculations:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

result = con.execute("""
    SELECT 
        city, 
        ST_X(ST_GeomFromWKB(geometry)) as longitude, 
        ST_Y(ST_GeomFromWKB(geometry)) as latitude
    FROM read_parquet('cities.geoparquet')
""").fetchall()

print("\nData read from GeoParquet using DuckDB:")
for row in result:
    print(f"City: {row[0]}, Longitude: {row[1]}, Latitude: {row[2]}")

# Calculate distances using DuckDB
result_distances = con.execute("""
    WITH cities AS (
        SELECT 
            city, 
            ST_GeomFromWKB(geometry) as geom
        FROM read_parquet('cities.geoparquet')
    )
    SELECT 
        city, 
        ST_Distance(geom, ST_Point(139.6917, 35.6895))/1000 as distance_to_tokyo_km
    FROM cities
""").fetchall()

print("\nDistances to Tokyo calculated by DuckDB (in kilometers):")
for row in result_distances:
    print(f"City: {row[0]}, Distance: {row[1]:.2f} km")

con.close()
```

## 5. Comparison of Approaches

1. **GeoPandas**: 
   - Pros: Native support for geospatial operations, intuitive for those familiar with pandas.
   - Cons: Can be memory-intensive for large datasets.

2. **Polars**: 
   - Pros: Very fast, good for large datasets.
   - Cons: Requires manual handling of geometry data, less built-in support for geospatial operations.

3. **DuckDB**: 
   - Pros: SQL interface, efficient for large datasets, built-in geospatial functions.
   - Cons: Requires knowledge of SQL and specific DuckDB functions.

Each approach has its strengths, and the choice depends on your specific use case, dataset size, and familiarity with the tools.

## 6. Best Practices and Tips

1. **Choose the Right Tool**: Consider your dataset size, query complexity, and performance requirements when choosing between GeoPandas, Polars, and DuckDB.

2. **Leverage GeoParquet's Efficiency**: Use GeoParquet for storing large geospatial datasets to take advantage of its compression and efficient querying capabilities.

3. **Understand Geometry Formats**: Be aware of how different tools handle geometry data (e.g., WKB in GeoParquet, native geometry objects in GeoPandas).

4. **Use Appropriate Projections**: When calculating distances or areas, make sure to use an appropriate projection for your data's geographic extent.

5. **Handle Large Datasets Carefully**: For very large datasets, consider using tools like DuckDB or Polars that are designed for out-of-memory processing.

6. **Validate Results**: Cross-check results between different tools, especially when implementing custom geospatial operations.

## 7. Conclusion and Next Steps

This tutorial has introduced you to working with GeoParquet data using Python, GeoPandas, Polars, and DuckDB. You've learned how to:

- Create and save GeoParquet files
- Read and process GeoParquet data using different tools
- Perform basic spatial operations and queries
- Calculate distances using different methods

To further your learning, consider exploring:

- More complex geospatial analyses and operations
- Handling larger datasets and optimizing performance
- Integrating these tools into data processing pipelines
- Visualizing geospatial data using libraries like Folium or Geopandas' plotting capabilities

Remember, the field of geospatial data processing is vast and constantly evolving. Keep exploring and experimenting with different tools and techniques to find the best solutions for your specific needs.

Happy geospatial data processing!
