# Python + GeoParquet + DuckDB Tutorial

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

## 2. Setting Up the Environment

### 2.1 Installation

To set up our environment, we need to install the following packages:

```bash
pip install geopandas pyarrow duckdb pandas polars
```

### 2.2 Importing Necessary Modules

In your Python script or Jupyter notebook, start with these imports:

```python
import geopandas as gpd
import pandas as pd
import polars as pl
import pyarrow as pa
import duckdb
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

While this adds a layer of complexity to our initial demo, it's an important concept in working with geospatial data in various systems. Different tools and databases may store and interpret geometry data in different ways, and functions like `ST_GeomFromWKB` allow us to bridge these differences.

### 3.2 Explanation of the Code

1. We create a simple GeoDataFrame with three cities and their coordinates.
2. We save this GeoDataFrame as a GeoParquet file.
3. We connect to DuckDB and enable its spatial extension.
4. We use SQL to read the GeoParquet file:
   - `read_parquet('cities.geoparquet')` reads the file.
   - `ST_GeomFromWKB(geometry)` converts the WKB geometry to a DuckDB geometry.
   - `ST_X` and `ST_Y` extract the longitude and latitude from the geometry.
5. We print the results, showing the city names and their coordinates.

This demonstration shows how we can seamlessly work with geospatial data across different tools - creating data with GeoPandas, storing it efficiently with GeoParquet, and querying it using DuckDB's SQL interface.

## 4. Next Steps

With this foundation, you can explore more advanced topics such as:
- Working with larger GeoParquet datasets
- Performing complex geospatial queries using DuckDB
- Comparing performance between different tools (pandas, GeoPandas, DuckDB)
- Applying these techniques to real-world geospatial analysis problems

Remember, while the use of `ST_GeomFromWKB` adds some complexity, it's a common pattern when working with geospatial data across different systems and formats. As you progress, you'll find this understanding valuable in various geospatial data processing scenarios.

Happy coding and geospatial analysis!
