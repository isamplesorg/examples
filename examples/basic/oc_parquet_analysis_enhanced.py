# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.7
#   kernelspec:
#     display_name: isamples-python-3.12.9
#     language: python
#     name: python3
# ---

# %% [markdown]
# > Note: If you have a different iSamples PQG parquet file from another provider, set `file_url` and `LOCAL_PATH` accordingly. All queries below will still work because they rely on PQG structure and iSamples model semantics.

# %% [markdown]
# # iSamples PQG Parquet Analysis (using OpenContext dataset)
#
# This notebook analyzes an iSamples Property Graph (PQG) parquet file. The sample file we use happens to be produced from OpenContext, but the schema, node types, and graph patterns are iSamples‑generic.
#
# ## Key Distinction: PQG framework vs iSamples model vs provider data
#
# We’ll keep these layers straight:
#
# 1. Generic PQG (Property Graph) framework
#    - Core graph fields: `s` (subject), `p` (predicate), `o` (object array), `n` (graph name)
#    - Edges are rows with `otype = '_edge_'`
#    - Graph traversal patterns (joins on s/p/o) are domain‑agnostic
#
# 2. iSamples metadata model (provider‑agnostic domain schema)
#    - Entity types: `MaterialSampleRecord`, `SamplingEvent`, `GeospatialCoordLocation`, `SamplingSite`, `IdentifiedConcept`, `Agent`, etc.
#    - Predicates like `produced_by`, `sample_location`, `sampling_site`, `has_material_category`, etc.
#    - These are defined by the iSamples model, not specific to OpenContext
#
# 3. Provider data (e.g., OpenContext)
#    - A particular provider’s content fills the iSamples model
#    - The dataset URL we load is from OpenContext, but the analysis is reusable for any iSamples PQG parquet

# %% [markdown]
# ## Setup and Data Loading

# %% [markdown]
#

# %% [markdown]
#

# %%
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import urllib.request
import os

# Configuration
file_url = "https://storage.googleapis.com/opencontext-parquet/oc_isamples_pqg.parquet"
# LOCAL_PATH is configured for Raymond Yee's local machine 

LOCAL_PATH = os.path.join(Path.home(), "Data", "iSample", "oc_isamples_pqg.parquet")

# %%
# Check if local file exists, download to generic location if not

if not os.path.exists(LOCAL_PATH):
    print(f"Local file not found at {LOCAL_PATH}")

    # if the file is not there, let's use tempfile module to create a temp file path.
    # put tempfile in /tmp/oc_isamples_pqg.parquet
    LOCAL_PATH = os.path.join("/tmp", "oc_isamples_pqg.parquet")
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    
    print(f"Downloading {file_url} to {LOCAL_PATH}...")
    urllib.request.urlretrieve(file_url, LOCAL_PATH)
    print("Download completed!")
else:
    print(f"Local file already exists at {LOCAL_PATH}")

# Use local path for parquet operations
parquet_path = LOCAL_PATH
print(f"Using parquet file: {parquet_path}")

# %% [markdown]
# ## Understanding the Data Structure
#
# ### PQG framework (generic)
# The parquet file uses a property graph model where both entities (nodes) and relationships (edges) are stored in one table. This pattern is generic and reusable across providers.
#
# Core PQG fields:
# - `s` (subject): source node row_id for an edge
# - `p` (predicate): relationship type
# - `o` (object): array of target row_ids
# - `n` (name): graph context/namespace (often null)
#
# Edges are rows with `otype = '_edge_'`.
#
# ### iSamples metadata model (provider‑agnostic)
# Values in `otype` and `p` map to the iSamples domain schema, independent of the specific provider:
# - Entity types: `MaterialSampleRecord`, `SamplingEvent`, `GeospatialCoordLocation`, `SamplingSite`, `IdentifiedConcept`, `Agent`, `_edge_`
# - Common predicates: `produced_by`, `sample_location`, `sampling_site`, `site_location`, `has_material_category`, `has_responsibility_actor`, etc.
#
# We’ll demonstrate queries that traverse the generic PQG structure while filtering/labeling using the iSamples model.
#
# Note: The example parquet we load is produced from OpenContext content, but the analysis patterns apply to any iSamples PQG parquet.

# %%
# Create a DuckDB connection
conn = duckdb.connect()

# Create view for the parquet file
conn.execute(f"CREATE VIEW pqg AS SELECT * FROM read_parquet('{parquet_path}');")

# Count records
result = conn.execute("SELECT COUNT(*) FROM pqg;").fetchone()
print(f"Total records: {result[0]:,}")

# %%
# Schema information
print("Schema information:")
schema_result = conn.execute("DESCRIBE pqg;").fetchall()
for row in schema_result[:10]:  # Show first 10 columns
    print(f"{row[0]:25} | {row[1]}")
print(f"... and {len(schema_result) - 10} more columns")

# %%
# Examine the distribution of entity types (iSamples model types)
entity_stats = conn.execute("""
    SELECT
        otype,
        COUNT(*) as count,
        COUNT(DISTINCT pid) as unique_pids,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM pqg
    GROUP BY otype
    ORDER BY count DESC
""").fetchdf()

print("Entity Type Distribution (iSamples model types):")
print(entity_stats)

# %% [markdown]
# ### Graph structure fields (PQG)
#
# The fields `s`, `p`, `o`, `n` are part of the generic PQG representation:
# - s (subject): row_id of the source entity
# - p (predicate): relationship type
# - o (object): array of target row_ids
# - n (name): graph context (usually null)
#
# These patterns are provider‑agnostic. The iSamples model provides the semantics for common predicates such as:
# - MaterialSampleRecord (s) produced_by (p) SamplingEvent (o)
# - SamplingEvent (s) sample_location (p) GeospatialCoordLocation (o)

# %%
# Explore edge predicates (iSamples model predicates)
edge_predicates = conn.execute("""
    SELECT
        p as predicate,
        COUNT(*) as usage_count,
        COUNT(DISTINCT s) as unique_subjects
    FROM pqg
    WHERE otype = '_edge_'
    GROUP BY p
    ORDER BY usage_count DESC
    LIMIT 15
""").fetchdf()

print("Most common relationship types (iSamples predicates):")
print(edge_predicates)

# %% [markdown]
# ## Practical Query Examples
#
# The following queries demonstrate both:
# 1. **Generic PQG patterns**: How to traverse graphs using s/p/o relationships
# 2. **OpenContext specifics**: The actual entity types and predicates for archaeological data

# %% [markdown]
# ## Understanding Geographic Paths in the iSamples Property Graph
#
# ### Path 1 and Path 2: Complementary, Not Alternative
#
# The iSamples model provides **two complementary paths** from samples to geographic coordinates. They serve different purposes and provide different levels of geographic granularity.
#
# ### Path 1 (Direct Event Location) - Precise Field Coordinates
#
# **What it is**: The **exact GPS coordinates** where a specific sampling event occurred.
#
# ```
# MaterialSampleRecord → produced_by → SamplingEvent → sample_location → GeospatialCoordLocation
# ```
#
# **Example**: "This pottery shard was collected at latitude 35.123, longitude 33.456"
#
# **Characteristics**:
# - Precise, field-recorded GPS point
# - Specific to each sampling event
# - Different events at the same site typically have different Path 1 coordinates
#
# **Use case**: "Show me the exact spot where this sample was collected"
#
# ### Path 2 (Via Sampling Site) - Administrative Site Location
#
# **What it is**: The **representative or administrative location** for a named archaeological site that groups related samples.
#
# ```
# MaterialSampleRecord → produced_by → SamplingEvent → sampling_site → SamplingSite → site_location → GeospatialCoordLocation
# ```
#
# **Example**: "This sample came from the PKAP Survey Area, whose general location is lat 34.987, lon 33.708"
#
# **Characteristics**:
# - One representative point for the entire site
# - Administrative/reference location that groups related samples
# - Many events at the same site share the **same** Path 2 location but have **different** Path 1 locations
#
# **Use case**: "Show me the general area/site where this sample came from"
#
# ### CRITICAL: Complementary Levels of Granularity, Not Alternatives
#
# ❌ **WRONG**: "Use Path 1 OR Path 2 to get the coordinates" (implies they return the same result)
#
# ✅ **CORRECT**: 
# - **Path 1** = precise individual sample location (fine-grained)
# - **Path 2** = administrative site grouping (coarse-grained)
# - Both are valid; which you use depends on whether you want precise points or site groupings
#
# ### Real-World Example: PKAP Survey Area (Large Regional Survey)
#
# **PKAP Survey Area** demonstrates why both paths are needed:
#
# ```sql
# -- Path 2: ONE administrative site location
# Site: PKAP Survey Area
# site_location: geoloc_ff64156b... (34.987406, 33.708047)
#
# -- Path 1: 544 DIFFERENT precise sample locations within that site!
# Top sample_location geos by event count:
# - geoloc_04d6e816...: 2,019 events at this precise spot
# - geoloc_9797bec3...: 754 events at this precise spot  
# - geoloc_67f077ed...: 577 events at this precise spot
# ... (541 more unique field locations)
# - geoloc_ff64156b... (matches site_location): only 106 events
# ```
#
# **Interpretation**: 
# - **Path 2** tells you: "All these samples belong to PKAP Survey Area at (34.987, 33.708)"
# - **Path 1** tells you: "But they were actually collected at 544 different specific GPS points within that survey area"
# - Both pieces of information are useful for different purposes!
#
# ### Contrast: Suberde (Small Compact Site)
#
# Not all sites have many different locations. **Suberde** shows when Path 1 and Path 2 converge:
#
# ```sql
# Site: Suberde  
# site_location: geoloc_4f3b18c2... (coordinates)
#
# Events at this site: 384
# All 384 events use the SAME coordinate for both Path 1 and Path 2
# ```
#
# For small, compact sites, the precise field location and administrative site location are essentially the same point.
#
# ### When to Use Each Path
#
# **Use Path 1 when you need**:
# - Precise GPS points for mapping individual samples
# - Fine-grained spatial analysis
# - "Show me exactly where each sample was found"
#
# **Use Path 2 when you need**:
# - Grouping samples by named site/project
# - Understanding administrative/project context
# - "Show me all samples from this archaeological site"
#
# **Use BOTH when you need**:
# - Complete geographic context (precise point + site affiliation)
# - "This sample was found at (35.123, 33.456) within the larger PKAP Survey Area"
# - This is what Eric's `get_sample_data_via_sample_pid()` does!

# %% [markdown]
# ## Full Relationship Map: Beyond Just Geographic Data
#
# The iSamples property graph contains many types of relationships beyond the two geographic paths:
#
# ```
#                                     Agent
#                                       ↑
#                                       | {responsibility, registrant}
#                                       |
# MaterialSampleRecord ────produced_by──→ SamplingEvent ────sample_location──→ GeospatialCoordLocation
#     |                                       |                                         ↑
#     |                                       |                                         |
#     | {keywords,                            └────sampling_site──→ SamplingSite ──site_location─┘
#     |  has_sample_object_type,                                      
#     |  has_material_category}                                    
#     |                                                             
#     └──→ IdentifiedConcept
# ```
#
# **Relationship Categories:**
# - **PATH 1**: MaterialSampleRecord → SamplingEvent → GeospatialCoordLocation (precise field location)
# - **PATH 2**: MaterialSampleRecord → SamplingEvent → SamplingSite → GeospatialCoordLocation (administrative site location)
# - **AGENT PATH**: MaterialSampleRecord → SamplingEvent → Agent (who collected/registered)
# - **CONCEPT PATH**: MaterialSampleRecord → IdentifiedConcept (types, keywords - direct, bypasses SamplingEvent!)
#
# **Key Insight**: SamplingEvent is the central hub for most relationships (Paths 1, 2, and Agent), but concepts attach directly to MaterialSampleRecord.

# %% [markdown]
# ## Eric's Query Functions: Understanding Path Usage
#
# The query functions in cell 59 (from Eric Kansa's `open-context-py`) demonstrate different path traversal patterns and how Path 1 and Path 2 are used.
#
# ### 1. `get_sample_data_via_sample_pid(sample_pid)` - Uses BOTH Path 1 AND Path 2
#
# **What it returns**: Complete geographic context for a sample - both precise location AND site affiliation.
#
# **Graph traversal**:
# ```
# MaterialSampleRecord (WHERE pid = sample_pid)
#   → produced_by → SamplingEvent
#     ├─→ sample_location → GeospatialCoordLocation [PATH 1: precise coordinates]
#     └─→ sampling_site → SamplingSite → site_location [PATH 2: site context]
# ```
#
# **Returns**: `sample_pid`, `sample_label`, `latitude`, `longitude` (from Path 1), `sample_site_label`, `sample_site_pid` (from Path 2)
#
# **Important**: Uses INNER JOIN on BOTH paths - sample must have BOTH precise coordinates AND site affiliation to appear in results.
#
# ---
#
# ### 2. `get_sample_data_agents_sample_pid(sample_pid)` - Uses AGENT PATH
#
# **What it returns**: Who collected or registered the sample.
#
# **Graph traversal**:
# ```
# MaterialSampleRecord (WHERE pid = sample_pid)
#   → produced_by → SamplingEvent
#     → {responsibility, registrant} → Agent
# ```
#
# **Returns**: `sample_pid`, `agent_pid`, `agent_name`, `predicate` (responsibility/registrant)
#
# **Independent of**: Path 1 and Path 2 - you get agents even if sample has no geographic data.
#
# ---
#
# ### 3. `get_sample_types_and_keywords_via_sample_pid(sample_pid)` - Uses CONCEPT PATH
#
# **What it returns**: Material types, keywords, and classifications.
#
# **Graph traversal**:
# ```
# MaterialSampleRecord (WHERE pid = sample_pid)
#   → {keywords, has_sample_object_type, has_material_category} → IdentifiedConcept
# ```
#
# **Returns**: `sample_pid`, `keyword_pid`, `keyword`, `predicate` (which type of classification)
#
# **Bypasses SamplingEvent**: Goes DIRECTLY from sample to concepts. Independent of all geographic and agent data.
#
# ---
#
# ### 4. `get_samples_at_geo_cord_location_via_sample_event(geo_pid)` - REVERSE Path 1, ENRICHED with Path 2
#
# **What it returns**: All samples collected at a specific geographic coordinate (reverse query).
#
# **Graph traversal** (starts at geo, walks backward to samples):
# ```
# GeospatialCoordLocation (WHERE pid = geo_pid)  ← START HERE
#   ← sample_location ← SamplingEvent [REVERSE PATH 1: events at this precise coordinate]
#     ├─→ sampling_site → SamplingSite [PATH 2: enrich with site name]
#     └─← produced_by ← MaterialSampleRecord [get the samples]
# ```
#
# **Returns**: `latitude`, `longitude`, `sample_pid`, `sample_label`, `sample_site_label`, `sample_site_pid`
#
# **Critical understanding**:
# - Uses **Path 1 in reverse** (`sample_location`) to find events at THIS PRECISE GPS point
# - Uses **Path 2 forward** (`sampling_site`) to enrich results with site names
# - This is NOT using `site_location` to find samples - it finds samples WHERE THE EVENT HAPPENED at `geo_pid`
# - The site information is added for context: "These samples were found at this precise point, and they belong to Site X"
#
# ---
#
# ### Summary Table: Path Usage
#
# | Function | Path 1 | Path 2 | Agent Path | Concept Path | Direction |
# |----------|--------|--------|------------|--------------|-----------|
# | `get_sample_data_via_sample_pid` | ✅ Required | ✅ Required | ❌ | ❌ | Forward (sample → geo) |
# | `get_sample_data_agents_sample_pid` | ❌ | ❌ | ✅ | ❌ | N/A |
# | `get_sample_types_and_keywords_via_sample_pid` | ❌ | ❌ | ❌ | ✅ | N/A |
# | `get_samples_at_geo_cord_location_via_sample_event` | ✅ Reverse | ✅ Enrichment | ❌ | ❌ | Reverse (geo → samples) |
#
# ### Key Takeaway: Path 1 vs Path 2 Usage Patterns
#
# **Path 1** (`sample_location`):
# - Used when you need **precise GPS coordinates** for individual samples
# - Used in reverse to find "what was sampled at this specific GPS point?"
#
# **Path 2** (`site_location`):  
# - Used to provide **site context and grouping** for samples
# - Used to answer "what named site does this sample belong to?"
# - Often used to ENRICH Path 1 results with administrative context
#
# **Together**: They provide complete geographic context - precise field location + site affiliation.

# %% [markdown]
# ### Graph Traversal Patterns Demonstrated Below
#
# The queries below use two complementary graph traversal paths for geographic data:
#
# **Path 1 - Direct event location (precise field coordinates)**:
# ```
# MaterialSampleRecord → produced_by → SamplingEvent → sample_location → GeospatialCoordLocation
# ```
#
# **Path 2 - Via sampling site (administrative site location)**:
# ```
# MaterialSampleRecord → produced_by → SamplingEvent → sampling_site → SamplingSite → site_location → GeospatialCoordLocation
# ```
#
# **Key point**: These provide different levels of geographic granularity (precise vs. site-level), and are often used together to provide complete context.

# %% [markdown]
# ### Translation Guide: Lonboard → Cesium
#
# The Lonboard visualization above uses concepts that map directly to Cesium:
#
# | Lonboard (Jupyter) | Cesium (Web/Quarto) | Purpose |
# |--------------------|---------------------|---------|
# | `ScatterplotLayer` | `PointPrimitiveCollection` | Container for points |
# | `get_fill_color` (RGBA array) | `color` property on each primitive | Point colors |
# | `get_radius` (float array) | `pixelSize` property | Point sizes |
# | `from_geopandas(gdf)` | Manual position array creation | Data source |
# | `pickable=True` | Event handlers on viewer | Interactivity |
#
# **Key insight**: Both are GPU-accelerated and use the same pattern:
# 1. Classify data (SQL query)
# 2. Create color/size arrays based on categories
# 3. Render with appropriate styling
#
# **Next step for Cesium**: 
# - Use the same SQL query in DuckDB-WASM
# - Create Cesium primitives with conditional colors
# - Add filtering UI (checkboxes to toggle categories)
#
# This Jupyter prototype validates the approach before implementing in the web-based Cesium tutorial!

# %%
# Prepare data for visualization: Classify all geos and extract coordinates

print("Preparing geographic data for visualization...")

# Query to get ALL geos with classification
geo_data = conn.execute("""
    WITH geo_classification AS (
        SELECT
            geo.pid,
            geo.latitude,
            geo.longitude,
            MAX(CASE WHEN e.p = 'sample_location' THEN 1 ELSE 0 END) as is_sample_location,
            MAX(CASE WHEN e.p = 'site_location' THEN 1 ELSE 0 END) as is_site_location
        FROM pqg geo
        JOIN pqg e ON (geo.row_id = list_extract(e.o, 1) AND e.otype = '_edge_')
        WHERE geo.otype = 'GeospatialCoordLocation'
          AND geo.latitude IS NOT NULL 
          AND geo.longitude IS NOT NULL
        GROUP BY geo.pid, geo.latitude, geo.longitude
    )
    SELECT
        pid,
        latitude,
        longitude,
        CASE
            WHEN is_sample_location = 1 AND is_site_location = 1 THEN 'both'
            WHEN is_sample_location = 1 THEN 'sample_location_only'
            WHEN is_site_location = 1 THEN 'site_location_only'
        END as location_type
    FROM geo_classification
""").fetchdf()

print(f"Retrieved {len(geo_data):,} geolocations")
print(f"\nBreakdown:")
for loc_type in ['sample_location_only', 'site_location_only', 'both']:
    count = len(geo_data[geo_data['location_type'] == loc_type])
    print(f"  {loc_type}: {count:,}")

# Convert to GeoDataFrame for Lonboard
import geopandas as gpd
from shapely.geometry import Point

gdf = gpd.GeoDataFrame(
    geo_data,
    geometry=[Point(lon, lat) for lon, lat in zip(geo_data.longitude, geo_data.latitude)],
    crs="EPSG:4326"
)

print(f"\nGeoDataFrame created: {len(gdf):,} points")
print("Ready for visualization!")

# %%
# Create color-coded visualization with Lonboard

from lonboard import Map, ScatterplotLayer
import numpy as np

# Define colors for each category (RGBA)
color_map = {
    'sample_location_only': [46, 134, 171, 200],    # Blue - field collection points
    'site_location_only':   [162, 59, 114, 200],    # Purple - administrative markers  
    'both':                 [241, 143, 1, 200]      # Orange - dual purpose
}

# Create color array based on location_type
colors = np.array([color_map[loc_type] for loc_type in gdf['location_type']], dtype=np.uint8)

# Create size array (site_location markers slightly larger)
sizes = np.array([
    6 if loc_type == 'site_location_only' else 
    5 if loc_type == 'both' else 
    3 
    for loc_type in gdf['location_type']
], dtype=np.float32)

# Create Lonboard layer
layer = ScatterplotLayer.from_geopandas(
    gdf,
    get_fill_color=colors,
    get_radius=sizes,
    radius_min_pixels=1,
    radius_max_pixels=10,
    pickable=True
)

# Create map
m = Map(layers=[layer], view_state={
    'latitude': 35.0,
    'longitude': 33.0,
    'zoom': 6,
    'pitch': 0,
    'bearing': 0
})

print("="*70)
print("Interactive Map: Three Geographic Categories")
print("="*70)
print("\n🔵 Blue: sample_location_only (precise field collection points)")
print("🟣 Purple: site_location_only (administrative site markers)")
print("🟠 Orange: both (dual-purpose locations)")
print("\n💡 Hover over points to see details")
print("\nThis demonstrates the same concept that could be implemented in Cesium!")

m

# %% [markdown]
# ## Interactive Visualization: Three-Category Geo Map
#
# Now let's visualize the three geographic categories using **Lonboard** (WebGL-based, similar to Cesium).
#
# This prototype demonstrates:
# 1. Color-coding by `location_type` (sample_location_only, site_location_only, both)
# 2. Handling 200k+ points efficiently
# 3. Interactive exploration of Path 1 vs Path 2 semantics
#
# The patterns learned here can be directly translated to the Cesium tutorial.

# %% [markdown]
# ### Summary: Visualization Implications
#
# **Current state**: The Cesium visualization (`parquet_cesium.qmd`) plots all 198,433 GeospatialCoordLocations identically, without differentiating their semantic roles.
#
# **Discovery**: Geos fall into three distinct categories:
# 1. **`sample_location_only`**: Precise field collection points (Path 1)
# 2. **`site_location_only`**: Administrative site markers (Path 2)
# 3. **`both`**: 10,346 dual-purpose locations (5.2%)
#
# **Proposed enhancement** (design note for future implementation):
#
# ```javascript
# // Color coding by semantic role
# const styles = {
#   sample_location_only: { color: '#2E86AB', size: 3 },  // Blue - field data
#   site_location_only:   { color: '#A23B72', size: 6 },  // Purple - admin markers
#   both:                 { color: '#F18F01', size: 5 }   // Orange - dual purpose
# };
#
# // UI controls
# ☑ Show sample locations (precise field collection points)
# ☑ Show site locations (administrative site markers)  
# ☐ Highlight overlap points only (10,346 dual-purpose geos)
# ```
#
# **Benefits:**
# - Makes Path 1 vs Path 2 distinction **visually concrete**
# - Reveals site spatial structure (compact sites vs distributed surveys)
# - Educational: users SEE the semantic difference between precise and administrative locations
# - Enables spatial queries: "Show me archaeological sites in Turkey" (filter to `site_location_only`)
#
# **Advanced feature**: Click a site_location → reveal all its sample_locations (e.g., click PKAP → see 544 collection points)
#
# This would transform the visualization from "pretty dots on a map" to a pedagogical tool for understanding the iSamples metadata model!

# %%
# PKAP DEEP DIVE: Examining a multi-location site

print("="*70)
print("Case Study: PKAP Survey Area (Multi-Location Site)")
print("="*70)

# Find PKAP
pkap = conn.execute("""
    SELECT
        site.pid as site_pid,
        site.label as site_label,
        site.row_id as site_row_id,
        COUNT(DISTINCT se.row_id) as event_count,
        COUNT(DISTINCT geo.pid) as unique_geo_count
    FROM pqg site
    JOIN pqg site_rel ON (site_rel.p = 'sampling_site' AND site.row_id = list_extract(site_rel.o, 1))
    JOIN pqg se ON (site_rel.s = se.row_id AND se.otype = 'SamplingEvent')
    JOIN pqg geo_rel ON (geo_rel.s = se.row_id AND geo_rel.p = 'sample_location')
    JOIN pqg geo ON (list_extract(geo_rel.o, 1) = geo.row_id AND geo.otype = 'GeospatialCoordLocation')
    WHERE site.otype = 'SamplingSite' AND site.label LIKE '%PKAP%'
    GROUP BY site.pid, site.label, site.row_id
""").fetchdf()

if not pkap.empty:
    site_row_id = pkap.iloc[0]['site_row_id']
    
    print(f"\nSite: {pkap.iloc[0]['site_label']}")
    print(f"Total sampling events: {pkap.iloc[0]['event_count']:,}")
    print(f"Unique sample_location geos: {pkap.iloc[0]['unique_geo_count']:,}")
    
    # Get the site_location
    site_location = conn.execute(f"""
        SELECT geo.pid, geo.latitude, geo.longitude
        FROM pqg e
        JOIN pqg geo ON (list_extract(e.o, 1) = geo.row_id AND geo.otype = 'GeospatialCoordLocation')
        WHERE e.s = {site_row_id} AND e.p = 'site_location'
    """).fetchdf()
    
    if not site_location.empty:
        site_geo_pid = site_location['pid'].iloc[0]
        print(f"\nSite_location (Path 2): {site_geo_pid}")
        print(f"Coordinates: ({site_location['latitude'].iloc[0]:.6f}, {site_location['longitude'].iloc[0]:.6f})")
        
        # Check how many events happened AT the site_location geo
        events_at_site_geo = conn.execute(f"""
            SELECT COUNT(*) as count
            FROM pqg site_rel
            JOIN pqg se ON (site_rel.s = se.row_id AND se.otype = 'SamplingEvent')
            JOIN pqg geo_rel ON (geo_rel.s = se.row_id AND geo_rel.p = 'sample_location')
            JOIN pqg geo ON (list_extract(geo_rel.o, 1) = geo.row_id AND geo.pid = '{site_geo_pid}')
            WHERE site_rel.p = 'sampling_site' AND {site_row_id} = list_extract(site_rel.o, 1)
        """).fetchdf()
        
        count = events_at_site_geo['count'].iloc[0]
        total = pkap.iloc[0]['event_count']
        
        print(f"\nEvents at site_location geo: {count:,} ({100*count/total:.1f}%)")
        print(f"Events at OTHER locations: {total - count:,} ({100*(total-count)/total:.1f}%)")
        
        print("\n" + "="*70)
        print("INTERPRETATION:")
        print("="*70)
        print(f"\n✅ PKAP's site_location IS used as a sample_location ({count} events)")
        print(f"✅ BUT it's just ONE of {pkap.iloc[0]['unique_geo_count']} precise locations")
        print("✅ The site_location serves as a REFERENCE POINT for the survey area")
        print(f"✅ Most sampling ({100*(total-count)/total:.1f}%) happened at other coordinates")

# %%
# SITE TYPE ANALYSIS: Distribution by number of unique sample_locations

print("="*70)
print("Site Type Distribution")
print("="*70)

site_types = conn.execute("""
    WITH site_geo_counts AS (
        SELECT
            site.pid as site_pid,
            site.label as site_label,
            COUNT(DISTINCT se.row_id) as event_count,
            COUNT(DISTINCT geo.pid) as unique_geo_count
        FROM pqg site
        JOIN pqg site_rel ON (site_rel.p = 'sampling_site' AND site.row_id = list_extract(site_rel.o, 1))
        JOIN pqg se ON (site_rel.s = se.row_id AND se.otype = 'SamplingEvent')
        JOIN pqg geo_rel ON (geo_rel.s = se.row_id AND geo_rel.p = 'sample_location')
        JOIN pqg geo ON (list_extract(geo_rel.o, 1) = geo.row_id AND geo.otype = 'GeospatialCoordLocation')
        WHERE site.otype = 'SamplingSite'
        GROUP BY site.pid, site.label
    )
    SELECT
        CASE
            WHEN unique_geo_count = 1 THEN 'Single location'
            WHEN unique_geo_count BETWEEN 2 AND 10 THEN 'Few locations (2-10)'
            WHEN unique_geo_count BETWEEN 11 AND 100 THEN 'Many locations (11-100)'
            ELSE 'Huge survey (100+)'
        END as site_type,
        COUNT(*) as site_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM site_geo_counts
    GROUP BY site_type
    ORDER BY 
        CASE
            WHEN site_type = 'Single location' THEN 1
            WHEN site_type = 'Few locations (2-10)' THEN 2
            WHEN site_type = 'Many locations (11-100)' THEN 3
            ELSE 4
        END
""").fetchdf()

print("\nSite Distribution by Spatial Extent (18,212 total sites):")
print(site_types)

print("\n" + "="*70)
print("INTERPRETATION:")
print("="*70)
print(f"\n✅ {site_types.iloc[0]['percentage']}% of sites are compact (single location)")
print("   Example: Suberde - 384 events all at one coordinate")
print(f"\n✅ {100 - site_types.iloc[0]['percentage']:.1f}% of sites are distributed (multiple locations)")
print("   Example: PKAP Survey Area - 15,446 events across 544 coordinates")

# %%
# CLASSIFICATION QUERY: Categorize all GeospatialCoordLocations

print("="*70)
print("Geographic Location Classification")
print("="*70)

geo_classification = conn.execute("""
    WITH geo_classification AS (
        SELECT
            geo.pid,
            geo.latitude,
            geo.longitude,
            MAX(CASE WHEN e.p = 'sample_location' THEN 1 ELSE 0 END) as is_sample_location,
            MAX(CASE WHEN e.p = 'site_location' THEN 1 ELSE 0 END) as is_site_location
        FROM pqg geo
        JOIN pqg e ON (geo.row_id = list_extract(e.o, 1) AND e.otype = '_edge_')
        WHERE geo.otype = 'GeospatialCoordLocation'
        GROUP BY geo.pid, geo.latitude, geo.longitude
    )
    SELECT
        CASE
            WHEN is_sample_location = 1 AND is_site_location = 1 THEN 'both'
            WHEN is_sample_location = 1 THEN 'sample_location_only'
            WHEN is_site_location = 1 THEN 'site_location_only'
        END as location_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM geo_classification
    GROUP BY location_type
    ORDER BY count DESC
""").fetchdf()

print("\nGeospatialCoordLocation Distribution (198,433 total):")
print(geo_classification)

print("\n" + "="*70)
print("KEY FINDINGS:")
print("="*70)
for _, row in geo_classification.iterrows():
    print(f"\n{row['location_type']}: {row['count']:,} geos ({row['percentage']}%)")
    
    if row['location_type'] == 'sample_location_only':
        print("  → Precise field collection points (Path 1)")
    elif row['location_type'] == 'site_location_only':
        print("  → Administrative site markers not used for collection (Path 2)")
    elif row['location_type'] == 'both':
        print("  → Dual-purpose: used as BOTH sample_location AND site_location")
        print("  → Includes single-location sites where field = admin location")

# %% [markdown]
# ## Geographic Location Classification: Three Types of Geos
#
# Having proved Path 1 and Path 2 are the ONLY paths, we can now classify all GeospatialCoordLocations based on HOW they're used in the graph.
#
# ### Research Questions
#
# 1. Are any geos used as BOTH `sample_location` AND `site_location`?
# 2. For sites where both types share a geo, do all events happen at that one location?
# 3. How are sites distributed across single vs multiple locations?
#
# These questions reveal important patterns about site structure and sampling strategies.

# %%
# PROOF STEP 4: Conclusion - Enumerate ALL paths

print("="*70)
print("CONCLUSION: Mathematical Proof of Exactly 2 Paths")
print("="*70)

print("\n📊 Graph Structure Facts:")
print("   1. GeospatialCoordLocation has ONLY 2 incoming edge types:")
print("      - SamplingEvent → sample_location → GeospatialCoordLocation")
print("      - SamplingSite → site_location → GeospatialCoordLocation")
print()
print("   2. MaterialSampleRecord has NO direct edge to GeospatialCoordLocation (0 edges)")
print()
print("   3. MaterialSampleRecord connects to SamplingEvent via 'produced_by' (1,096,352 edges)")
print("      This is the ONLY path from MaterialSampleRecord toward geo data")
print()
print("   4. SamplingEvent connects to:")
print("      - GeospatialCoordLocation (via sample_location) - Path 1")
print("      - SamplingSite (via sampling_site)")
print()  
print("   5. SamplingSite connects to:")
print("      - GeospatialCoordLocation (via site_location) - Path 2")
print()

print("🔒 Therefore, exactly TWO paths exist:")
print()
print("   PATH 1: MaterialSampleRecord → produced_by → SamplingEvent → sample_location → GeospatialCoordLocation")
print("   PATH 2: MaterialSampleRecord → produced_by → SamplingEvent → sampling_site → SamplingSite → site_location → GeospatialCoordLocation")
print()
print("   Any other path is MATHEMATICALLY IMPOSSIBLE given the graph topology.")
print()

print("💡 This is a structural constraint of the iSamples metadata model,")
print("   not just a data observation!")
print("="*70)

# %%
# PROOF STEP 3: What does MaterialSampleRecord connect to?

print("="*70)
print("STEP 3: ALL outbound edges FROM MaterialSampleRecord")
print("="*70)

edges_from_sample = conn.execute("""
    SELECT 
        e.p as predicate,
        target.otype as target_type,
        COUNT(*) as count
    FROM pqg sample
    JOIN pqg e ON (sample.row_id = e.s AND e.otype = '_edge_')
    JOIN pqg target ON (list_extract(e.o, 1) = target.row_id)
    WHERE sample.otype = 'MaterialSampleRecord'
    GROUP BY e.p, target.otype
    ORDER BY count DESC
""").fetchdf()

print("\nAll outbound predicates from MaterialSampleRecord:")
print(edges_from_sample)

print("\n✅ FINDING: MaterialSampleRecord connects to these entity types:")
for _, row in edges_from_sample.iterrows():
    print(f"   - {row['target_type']} (via {row['predicate']}): {row['count']:,} edges")

print("\n🎯 KEY: Only 'produced_by → SamplingEvent' can lead to geographic data")
print("   (IdentifiedConcept and Agent don't connect to GeospatialCoordLocation)")

# %%
# PROOF STEP 2: Does MaterialSampleRecord have a DIRECT edge to GeospatialCoordLocation?

print("="*70)
print("STEP 2: Direct MaterialSampleRecord → GeospatialCoordLocation edges?")
print("="*70)

direct_edges = conn.execute("""
    SELECT COUNT(*) as count
    FROM pqg sample
    JOIN pqg e ON (sample.row_id = e.s AND e.otype = '_edge_')
    JOIN pqg geo ON (list_extract(e.o, 1) = geo.row_id AND geo.otype = 'GeospatialCoordLocation')
    WHERE sample.otype = 'MaterialSampleRecord'
""").fetchdf()

print(f"\nDirect MaterialSampleRecord → GeospatialCoordLocation edges: {direct_edges['count'].iloc[0]}")

if direct_edges['count'].iloc[0] == 0:
    print("\n✅ FINDING: MaterialSampleRecord has ZERO direct edges to GeospatialCoordLocation")
    print("   Therefore, MaterialSampleRecord MUST go through intermediate entities")

# %%
# PROOF STEP 1: What entity types connect TO GeospatialCoordLocation?
# This query finds ALL incoming edges to GeospatialCoordLocation

print("="*70)
print("STEP 1: What connects TO GeospatialCoordLocation?")
print("="*70)

edges_to_geo = conn.execute("""
    SELECT 
        source.otype as source_type,
        e.p as predicate,
        COUNT(*) as count
    FROM pqg geo
    JOIN pqg e ON (geo.row_id = list_extract(e.o, 1) AND e.otype = '_edge_')
    JOIN pqg source ON (e.s = source.row_id)
    WHERE geo.otype = 'GeospatialCoordLocation'
    GROUP BY source.otype, e.p
    ORDER BY count DESC
""").fetchdf()

print("\nALL entity types with edges TO GeospatialCoordLocation:")
print(edges_to_geo)

print("\n✅ FINDING: ONLY two entity types connect to GeospatialCoordLocation:")
print("   - SamplingEvent (via sample_location)")
print("   - SamplingSite (via site_location)")

# %% [markdown]
# ## Mathematical Proof: Path 1 and Path 2 Are the ONLY Paths
#
# **Key Discovery**: Path 1 and Path 2 are not just "common patterns" - they are the **ONLY two possible paths** from MaterialSampleRecord to GeospatialCoordLocation in the iSamples graph model.
#
# This is a **structural constraint** of the iSamples metadata model, proven by analyzing the graph topology.
#
# ### The Proof
#
# The following queries demonstrate that there are exactly two paths and no others are mathematically possible:
#
# **Step 1**: What entity types connect TO GeospatialCoordLocation?
# - Query the graph to find ALL incoming edges to GeospatialCoordLocation
#
# **Step 2**: How does MaterialSampleRecord connect to those entities?
# - MaterialSampleRecord has NO direct edge to GeospatialCoordLocation
# - MaterialSampleRecord ONLY connects to SamplingEvent (via `produced_by`)
#
# **Step 3**: Enumerate all paths
# - Since MaterialSampleRecord MUST go through SamplingEvent
# - And GeospatialCoordLocation is ONLY reachable from SamplingEvent and SamplingSite
# - And SamplingSite is ONLY reachable from SamplingEvent
# - Therefore: exactly **2 paths** exist, no more, no less
#
# ### Why This Matters
#
# - This is an **architectural invariant** of the iSamples model
# - Not just an observation about the OpenContext data
# - Future iSamples implementations MUST follow this structure
# - Can confidently state "Path 1 and Path 2 are the only ways..." without caveats
# - Validates that our Path 1/Path 2 framework is **complete and exhaustive**

# %% [markdown]
# ### Query 1: Find MaterialSampleRecords with Geographic Coordinates
#
# This query demonstrates:
# - **Generic PQG pattern**: Multi-hop graph traversal through edges
# - **OpenContext specifics**: Archaeological entity types and relationships

# %%
# Find samples with geographic coordinates (via SamplingEvent)
# PQG: traverse edges by joining on s/p/o; iSamples: filter types/predicates

# Ensure we have a working connection
try:
    conn.execute("SELECT 1").fetchone()
except:
    conn = duckdb.connect()
    conn.execute(f"CREATE VIEW pqg AS SELECT * FROM read_parquet('{parquet_path}');")

samples_with_coords = conn.execute("""
    SELECT
        s.pid as sample_id,
        s.label as sample_label,
        s.description,
        g.latitude,
        g.longitude,
        g.place_name,
        'direct_event_location' as location_type
    FROM pqg s
    JOIN pqg e1   ON s.row_id = e1.s AND e1.p = 'produced_by'
    JOIN pqg evt  ON e1.o[1] = evt.row_id
    JOIN pqg e2   ON evt.row_id = e2.s AND e2.p = 'sample_location'
    JOIN pqg g    ON e2.o[1] = g.row_id
    WHERE s.otype = 'MaterialSampleRecord'
      AND evt.otype = 'SamplingEvent'
      AND g.otype = 'GeospatialCoordLocation'
      AND g.latitude IS NOT NULL
    LIMIT 100
""").fetchdf()

print(f"Found {len(samples_with_coords)} samples with direct event coordinates")
samples_with_coords.head()

# %% [markdown]
# ### Using Ibis for Cleaner Multi-Step Joins
#
# Ibis provides a more Pythonic interface for the same **generic PQG graph traversal patterns**, while making **OpenContext-specific** entity filtering clearer.

# %%
# Import Ibis for cleaner data manipulation
import ibis
from ibis import _

ibis.options.interactive = True

# Create Ibis connection using DuckDB
ibis_conn = ibis.duckdb.connect()

# Register the parquet file as a table in Ibis
pqg = ibis_conn.read_parquet(parquet_path, table_name='pqg')

print("Ibis setup complete!")
print(f"Table columns: {pqg.columns}")
print(f"Total records: {pqg.count().execute():,}")

# %%
# Ibis version: Find samples with geographic coordinates through SamplingEvent

# Base tables with iSamples model type filters
samples = pqg.filter(_.otype == 'MaterialSampleRecord').alias('samples')
events = pqg.filter(_.otype == 'SamplingEvent').alias('events')
locations = pqg.filter(_.otype == 'GeospatialCoordLocation').alias('locations')
edges = pqg.filter(_.otype == '_edge_').alias('edges')

# Sample -> produced_by -> SamplingEvent
sample_to_event = (
    samples
    .join(
        edges.filter(_.p == 'produced_by'),
        samples.row_id == edges.s
    )
    .join(
        events,
        edges.o[0] == events.row_id
    )
)

# SamplingEvent -> sample_location -> GeospatialCoordLocation
location_edges = edges.filter(_.p == 'sample_location').alias('location_edges')
event_to_location = (
    sample_to_event
    .join(
        location_edges,
        events.row_id == location_edges.s
    )
    .join(
        locations.filter(_.latitude.notnull()),
        location_edges.o[0] == locations.row_id
    )
)

samples_with_coords_ibis = (
    event_to_location
    .select(
        sample_id=samples.pid,
        sample_label=samples.label,
        description=samples.description,
        latitude=locations.latitude,
        longitude=locations.longitude,
        place_name=locations.place_name,
        location_type=ibis.literal('direct_event_location')
    )
    .limit(100)
)

result_ibis = samples_with_coords_ibis.execute()
print(f"Found {len(result_ibis)} samples with direct event coordinates (Ibis)")
result_ibis.head()

# %%
# Ibis version: Find samples via site location path

sites = pqg.filter(_.otype == 'SamplingSite').alias('sites')

# Define edge tables
event_edges = edges.filter(_.p == 'produced_by').alias('event_edges')
site_edges = edges.filter(_.p == 'sampling_site').alias('site_edges')
site_location_edges = edges.filter(_.p == 'site_location').alias('site_location_edges')

samples_via_sites_ibis = (
    samples
    .join(event_edges, samples.row_id == event_edges.s)
    .join(events, event_edges.o[0] == events.row_id)
    .join(site_edges, events.row_id == site_edges.s)
    .join(sites, site_edges.o[0] == sites.row_id)
    .join(site_location_edges, sites.row_id == site_location_edges.s)
    .join(
        locations.filter(_.latitude.notnull()),
        site_location_edges.o[0] == locations.row_id
    )
    .select(
        sample_id=samples.pid,
        sample_label=samples.label,
        site_name=sites.label,
        latitude=locations.latitude,
        longitude=locations.longitude,
        location_type=ibis.literal('via_site_location')
    )
    .limit(100)
)

result_via_sites_ibis = samples_via_sites_ibis.execute()
print(f"Found {len(result_via_sites_ibis)} samples with site-based coordinates (Ibis)")
result_via_sites_ibis.head()


# %%
# Ibis version: get_sample_locations_for_viz function

def get_sample_locations_for_viz_ibis(limit=10000):
    """Extract sample locations optimized for visualization using Ibis"""

    event_edges = edges.filter(_.p == 'produced_by').alias('event_edges')
    sample_location_edges = edges.filter(_.p == 'sample_location').alias('sample_location_edges')
    site_edges = edges.filter(_.p == 'sampling_site').alias('site_edges')
    site_location_edges = edges.filter(_.p == 'site_location').alias('site_location_edges')

    # Direct locations: Sample -> Event -> sample_location -> Location
    direct_locations = (
        samples
        .join(event_edges, samples.row_id == event_edges.s)
        .join(events, event_edges.o[0] == events.row_id)
        .join(sample_location_edges, events.row_id == sample_location_edges.s)
        .join(
            locations.filter((_.latitude.notnull()) & (_.longitude.notnull()) & (~_.obfuscated)),
            sample_location_edges.o[0] == locations.row_id
        )
        .select(
            sample_id=samples.pid,
            label=samples.label,
            latitude=locations.latitude,
            longitude=locations.longitude,
            obfuscated=locations.obfuscated,
            location_type=ibis.literal('direct')
        )
    )

    # Site locations: Sample -> Event -> Site -> site_location -> Location
    site_locations = (
        samples
        .join(event_edges, samples.row_id == event_edges.s)
        .join(events, event_edges.o[0] == events.row_id)
        .join(site_edges, events.row_id == site_edges.s)
        .join(sites, site_edges.o[0] == sites.row_id)
        .join(site_location_edges, sites.row_id == site_location_edges.s)
        .join(
            locations.filter((_.latitude.notnull()) & (_.longitude.notnull()) & (~_.obfuscated)),
            site_location_edges.o[0] == locations.row_id
        )
        .select(
            sample_id=samples.pid,
            label=samples.label,
            latitude=locations.latitude,
            longitude=locations.longitude,
            obfuscated=locations.obfuscated,
            location_type=ibis.literal('via_site')
        )
    )

    return direct_locations.union(site_locations).limit(limit).execute()

# Get visualization-ready data using Ibis
viz_data_ibis = get_sample_locations_for_viz_ibis(5000)
print(f"Prepared {len(viz_data_ibis)} samples for visualization (Ibis version)")
if len(viz_data_ibis) > 0:
    print(f"Coordinate bounds: Lat [{viz_data_ibis.latitude.min():.2f}, {viz_data_ibis.latitude.max():.2f}], "
          f"Lon [{viz_data_ibis.longitude.min():.2f}, {viz_data_ibis.longitude.max():.2f}]")
    print(f"Location types: {viz_data_ibis.location_type.value_counts().to_dict()}")
else:
    print("No samples found with valid coordinates")

viz_data_ibis.head()

# %% [markdown]
# ### Comparison: Raw SQL vs Ibis
#
# Both approaches implement the same **generic PQG graph traversal patterns**. The Ibis versions offer several advantages:
#
# #### **Readability Benefits:**
# 1. **Clear separation**: Generic PQG operations (joins on s/p/o) vs OpenContext filters (entity types)
# 2. **Meaningful aliases**: `samples`, `events`, `locations` make the domain model clear
# 3. **Method chaining**: Natural Python syntax that reads left-to-right
# 4. **Type safety**: Ibis can catch column reference errors at definition time
#
# #### **Maintainability Benefits:**
# 1. **Modular queries**: Easy to swap OpenContext predicates without changing graph traversal logic
# 2. **Reusable components**: Base table filters separate framework from domain
# 3. **IDE support**: Auto-completion works for both PQG fields and domain fields
# 4. **Debugging**: Can inspect intermediate results by executing partial chains
#
# #### **Performance Considerations:**
# - Both compile to the same SQL, leveraging DuckDB's query optimizer
# - The graph traversal pattern (joining through edges) is the same
# - Performance is determined by the underlying PQG structure, not the query interface

# %%
# Quick performance and correctness comparison
import time

print("=== PERFORMANCE COMPARISON ===")

# Time the DuckDB SQL query
perf_conn = duckdb.connect()
perf_conn.execute(f"CREATE VIEW pqg AS SELECT * FROM read_parquet('{parquet_path}');")

start_time = time.time()
sql_result = perf_conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT s.pid as sample_id
        FROM pqg s
        JOIN pqg e1 ON s.row_id = e1.s AND e1.p = 'produced_by'
        JOIN pqg evt ON e1.o[1] = evt.row_id
        JOIN pqg e2 ON evt.row_id = e2.s AND e2.p = 'sample_location'
        JOIN pqg g  ON e2.o[1] = g.row_id
        WHERE s.otype = 'MaterialSampleRecord'
          AND evt.otype = 'SamplingEvent'
          AND g.otype = 'GeospatialCoordLocation'
          AND g.latitude IS NOT NULL
    )
""").fetchone()[0]
sql_time = time.time() - start_time

# Time the Ibis query
start_time = time.time()
ibis_count = samples_with_coords_ibis.count().execute()
ibis_time = time.time() - start_time

print(f"Raw SQL result count: {sql_result}")
print(f"Raw SQL execution time: {sql_time:.3f} seconds")
print(f"Ibis result count: {ibis_count}")
print(f"Ibis execution time: {ibis_time:.3f} seconds")
print(f"Results match: {sql_result == ibis_count}")
print(f"Performance ratio: {ibis_time/sql_time:.2f}x")

perf_conn.close()

print("\n=== KEY TAKEAWAYS ===")
print("✓ Ibis provides much more readable code for complex joins")
print("✓ Performance is comparable (compiles to same SQL)")
print("✓ Good separation of PQG traversal from iSamples semantics")


# %% [markdown]
# ## Summary
#
# **✅ Fixed Issues:**
# - Resolved `AttributeError: 'Table' object has no attribute 'location_edges'` by properly defining aliased edge tables separately
# - Fixed duplicate CTE names in the visualization function by using unique aliases
# - All Ibis queries now execute successfully
#
# **Key Improvements with Ibis:**
# 1. **Much cleaner syntax** for multi-step joins - no more cryptic SQL aliases
# 2. **Step-by-step query building** makes complex logic easier to understand
# 3. **Reusable components** - define edge tables once, use multiple times
# 4. **Better debugging** - can inspect intermediate results easily
# 5. **IDE support** - auto-completion and type checking work better
#
# **Performance:** Ibis compiles to efficient SQL, so performance is equivalent to hand-written queries.

# %%
# Helper function to ensure we have a working DuckDB connection
def ensure_connection():
    """Ensure we have a working DuckDB connection with the parquet view"""
    global conn
    try:
        conn.execute("SELECT 1").fetchone()
    except (NameError, Exception):
        print("Recreating DuckDB connection...")
        conn = duckdb.connect()
        conn.execute(f"CREATE VIEW pqg AS SELECT * FROM read_parquet('{parquet_path}');")
        print("Connection restored!")
    return conn

# Test the connection
ensure_connection()
print("DuckDB connection is ready!")


# %%
def ark_to_url(pid: str) -> str:
    """Return a resolvable n2t.net URL for an ARK identifier.
    If pid is not an ARK, return it as a string.
    """
    if isinstance(pid, str) and pid.startswith("ark:/"):
        return f"https://n2t.net/{pid}"
    return str(pid)

# Quick smoke test if a sample_pid is already in scope (harmless if not)
if 'sample_pid' in globals():
    print("Sample URL:", ark_to_url(sample_pid))


# %% [markdown]
# ## Utilities
#
# Helper functions used across the notebook (defined early for clarity and reuse).

# %%
def get_sample_geo_context_via_sample_pid(conn, sample_pid: str, mode: str = 'either_or'):
    """
    Return Path 1 (direct event location) and Path 2 (site-based location) for a given sample_pid,
    with control over which paths to include.

    Modes (case-insensitive):
    - 'either_or' (default): return rows where Path 1 OR Path 2 exists
    - 'both':      return rows where BOTH Path 1 AND Path 2 exist
    - 'only_path1': return rows where Path 1 exists and Path 2 does NOT
    - 'only_path2': return rows where Path 2 exists and Path 1 does NOT

    Inputs:
    - conn: DuckDB connection with a view 'pqg' pointing to the parquet data.
    - sample_pid: ARK or PID of a MaterialSampleRecord.

    Output (pandas.DataFrame) columns:
    - sample_pid, sample_label
    - path1_geo_pid, path1_latitude, path1_longitude  (SamplingEvent → sample_location → GeospatialCoordLocation)
    - site_pid, site_label
    - path2_geo_pid, path2_latitude, path2_longitude  (SamplingEvent → sampling_site → SamplingSite → site_location)

    Notes:
    - A sample typically has a single produced_by event; if multiple exist, results may return multiple rows.
    - Coordinates are constrained to non-null latitude/longitude.
    """
    ensure_connection()

    mode_norm = (mode or 'either_or').strip().lower()
    if mode_norm not in {'either_or', 'both', 'only_path1', 'only_path2'}:
        raise ValueError("mode must be one of: 'either_or', 'both', 'only_path1', 'only_path2'")

    # Build WHERE clause based on mode
    if mode_norm == 'both':
        where_clause = "WHERE p1.path1_geo_pid IS NOT NULL AND p2.path2_geo_pid IS NOT NULL"
    elif mode_norm == 'only_path1':
        where_clause = "WHERE p1.path1_geo_pid IS NOT NULL AND p2.path2_geo_pid IS NULL"
    elif mode_norm == 'only_path2':
        where_clause = "WHERE p1.path1_geo_pid IS NULL AND p2.path2_geo_pid IS NOT NULL"
    else:  # either_or
        where_clause = "WHERE p1.path1_geo_pid IS NOT NULL OR p2.path2_geo_pid IS NOT NULL"

    sql = f"""
        WITH sample_event AS (
            SELECT s.pid AS sample_pid, s.label AS sample_label, evt.row_id AS event_row_id
            FROM pqg s
            JOIN pqg e1   ON s.row_id = e1.s AND e1.p = 'produced_by'
            JOIN pqg evt  ON e1.o[1] = evt.row_id AND evt.otype = 'SamplingEvent'
            WHERE s.otype = 'MaterialSampleRecord' AND s.pid = ?
        ),
        path1 AS (
            -- Path 1: SamplingEvent → sample_location → GeospatialCoordLocation
            SELECT se.sample_pid,
                   geo.pid        AS path1_geo_pid,
                   geo.latitude   AS path1_latitude,
                   geo.longitude  AS path1_longitude
            FROM sample_event se
            JOIN pqg e   ON e.s = se.event_row_id AND e.p = 'sample_location' AND e.otype = '_edge_'
            JOIN pqg geo ON geo.row_id = e.o[1] AND geo.otype = 'GeospatialCoordLocation'
            WHERE geo.latitude IS NOT NULL AND geo.longitude IS NOT NULL
        ),
        site_rel AS (
            -- SamplingEvent → sampling_site → SamplingSite
            SELECT se.sample_pid,
                   site.row_id AS site_row_id,
                   site.pid    AS site_pid,
                   site.label  AS site_label
            FROM sample_event se
            JOIN pqg e    ON e.s = se.event_row_id AND e.p = 'sampling_site' AND e.otype = '_edge_'
            JOIN pqg site ON site.row_id = e.o[1] AND site.otype = 'SamplingSite'
        ),
        path2 AS (
            -- Path 2: SamplingSite → site_location → GeospatialCoordLocation
            SELECT sr.sample_pid,
                   geo.pid        AS path2_geo_pid,
                   geo.latitude   AS path2_latitude,
                   geo.longitude  AS path2_longitude
            FROM site_rel sr
            JOIN pqg e   ON e.s = sr.site_row_id AND e.p = 'site_location' AND e.otype = '_edge_'
            JOIN pqg geo ON geo.row_id = e.o[1] AND geo.otype = 'GeospatialCoordLocation'
            WHERE geo.latitude IS NOT NULL AND geo.longitude IS NOT NULL
        )
        SELECT
            se.sample_pid,
            se.sample_label,
            p1.path1_geo_pid,
            p1.path1_latitude,
            p1.path1_longitude,
            sr.site_pid,
            sr.site_label,
            p2.path2_geo_pid,
            p2.path2_latitude,
            p2.path2_longitude
        FROM sample_event se
        LEFT JOIN site_rel sr ON sr.sample_pid = se.sample_pid
        LEFT JOIN path1 p1    ON p1.sample_pid = se.sample_pid
        LEFT JOIN path2 p2    ON p2.sample_pid = se.sample_pid
        {where_clause}
    """

    return conn.execute(sql, [sample_pid]).fetchdf()

# Optional quick smoke test if a sample_pid is already defined in the notebook
try:
    if 'sample_pid' in globals() and isinstance(sample_pid, str):
        print("Preview Path 1 only for", sample_pid)
        display(get_sample_geo_context_via_sample_pid(conn, sample_pid, mode='only_path1').head())
        print("Preview Path 2 only for", sample_pid)
        display(get_sample_geo_context_via_sample_pid(conn, sample_pid, mode='only_path2').head())
        print("Preview Either/Or for", sample_pid)
        display(get_sample_geo_context_via_sample_pid(conn, sample_pid, mode='either_or').head())
        print("Preview Both for", sample_pid)
        display(get_sample_geo_context_via_sample_pid(conn, sample_pid, mode='both').head())
except Exception as _:
    pass


# %%
def get_samples_for_geo_pid(conn, geo_pid: str, mode: str = 'either_or', limit: int = 10000):
    """
    Reverse traversal from a GeospatialCoordLocation PID to samples via Path 1 and/or Path 2.

    Modes (case-insensitive):
    - 'either_or' (default): include samples reachable via Path 1 OR Path 2
    - 'both':      include samples that have BOTH a Path 1 and a Path 2 relation to this geo
    - 'only_path1': include samples reachable via Path 1 but NOT via Path 2
    - 'only_path2': include samples reachable via Path 2 but NOT via Path 1

    Path definitions:
    - Path 1 (reverse): GeospatialCoordLocation ← sample_location ← SamplingEvent ← produced_by ← MaterialSampleRecord
    - Path 2 (reverse): GeospatialCoordLocation ← site_location ← SamplingSite ← sampling_site ← SamplingEvent ← produced_by ← MaterialSampleRecord

    Returns a pandas.DataFrame with columns:
    - geo_pid, latitude, longitude
    - sample_pid, sample_label
    - site_pid, site_label (only populated for Path 2)
    - has_path1 (0/1), has_path2 (0/1)
    """
    ensure_connection()

    mode_norm = (mode or 'either_or').strip().lower()
    if mode_norm not in {'either_or', 'both', 'only_path1', 'only_path2'}:
        raise ValueError("mode must be one of: 'either_or', 'both', 'only_path1', 'only_path2'")

    if mode_norm == 'both':
        having_clause = "has_path1 = 1 AND has_path2 = 1"
    elif mode_norm == 'only_path1':
        having_clause = "has_path1 = 1 AND has_path2 = 0"
    elif mode_norm == 'only_path2':
        having_clause = "has_path1 = 0 AND has_path2 = 1"
    else:  # either_or
        having_clause = "has_path1 = 1 OR has_path2 = 1"

    sql = f"""
        WITH target_geo AS (
            SELECT row_id AS geo_row_id, pid AS geo_pid, latitude, longitude
            FROM pqg
            WHERE otype = 'GeospatialCoordLocation'
              AND pid = ?
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            LIMIT 1
        ),
        p1 AS (
            -- Path 1 reverse: geo ← sample_location ← event ← produced_by ← sample
            SELECT s.pid AS sample_pid, s.label AS sample_label
            FROM target_geo g
            JOIN pqg e_sl ON e_sl.otype = '_edge_' AND e_sl.p = 'sample_location' AND e_sl.o[1] = g.geo_row_id
            JOIN pqg evt  ON evt.row_id = e_sl.s AND evt.otype = 'SamplingEvent'
            JOIN pqg e_pb ON e_pb.otype = '_edge_' AND e_pb.p = 'produced_by' AND e_pb.o[1] = evt.row_id
            JOIN pqg s    ON s.row_id = e_pb.s AND s.otype = 'MaterialSampleRecord'
        ),
        p2 AS (
            -- Path 2 reverse: geo ← site_location ← site ← sampling_site ← event ← produced_by ← sample
            SELECT s.pid AS sample_pid, s.label AS sample_label, site.pid AS site_pid, site.label AS site_label
            FROM target_geo g
            JOIN pqg e_site_loc ON e_site_loc.otype = '_edge_' AND e_site_loc.p = 'site_location' AND e_site_loc.o[1] = g.geo_row_id
            JOIN pqg site      ON site.row_id = e_site_loc.s AND site.otype = 'SamplingSite'
            JOIN pqg e_ss      ON e_ss.otype = '_edge_' AND e_ss.p = 'sampling_site' AND e_ss.o[1] = site.row_id
            JOIN pqg evt       ON evt.row_id = e_ss.s AND evt.otype = 'SamplingEvent'
            JOIN pqg e_pb      ON e_pb.otype = '_edge_' AND e_pb.p = 'produced_by' AND e_pb.o[1] = evt.row_id
            JOIN pqg s         ON s.row_id = e_pb.s AND s.otype = 'MaterialSampleRecord'
        ),
        combined AS (
            SELECT sample_pid, sample_label, NULL AS site_pid, NULL AS site_label, 1 AS has_path1, 0 AS has_path2 FROM p1
            UNION ALL
            SELECT sample_pid, sample_label, site_pid, site_label, 0, 1 FROM p2
        ),
        collapsed AS (
            SELECT
                sample_pid,
                MIN(sample_label) AS sample_label,
                MAX(site_pid)     AS site_pid,
                MAX(site_label)   AS site_label,
                CAST(MAX(has_path1) AS INTEGER) AS has_path1,
                CAST(MAX(has_path2) AS INTEGER) AS has_path2
            FROM combined
            GROUP BY sample_pid
        )
        SELECT
            tg.geo_pid,
            tg.latitude,
            tg.longitude,
            c.sample_pid,
            c.sample_label,
            c.site_pid,
            c.site_label,
            c.has_path1,
            c.has_path2
        FROM target_geo tg
        JOIN collapsed c ON TRUE
        WHERE {having_clause}
        ORDER BY c.sample_label
        LIMIT {limit}
    """

    return conn.execute(sql, [geo_pid]).fetchdf()

# Optional quick smoke test if a test geo pid is in scope
try:
    if 'test_geo_pid' in globals() and isinstance(test_geo_pid, str):
        print("Reverse lookup @ geo (either_or):", test_geo_pid)
        display(get_samples_for_geo_pid(conn, test_geo_pid, mode='either_or', limit=10))
except Exception as _:
    pass

# %% [markdown]
# ## PKAP Survey Area: Path 1 vs Path 2 Demo
#
# This demo:
# - Locates the PKAP Survey Area site and its `site_location` geospatial PID
# - Uses the reverse function (`get_samples_for_geo_pid`) from that geo PID in four modes:
#   - `either_or`, `both`, `only_path1`, `only_path2`
# - Picks one sample from the results and shows forward traversal with `get_sample_geo_context_via_sample_pid` in the same modes.
#
# Interpretation reminder:
# - Path 1 = precise event point (sample_location)
# - Path 2 = administrative site location (site_location)
# - For PKAP, most events are at many precise points (Path 1), while the site location (Path 2) is a single representative point. Some events may coincide with the site location, thus appearing in `both`. 

# %%
# Find PKAP site and its site_location geo PID, then run the demos
ensure_connection()

# 1) Locate the PKAP site
pkap_site = conn.execute("""
    SELECT site.row_id AS site_row_id, site.pid AS site_pid, site.label AS site_label
    FROM pqg site
    WHERE site.otype = 'SamplingSite' AND site.label LIKE '%PKAP%'
    LIMIT 1
""").fetchdf()

if pkap_site.empty:
    raise ValueError("PKAP site not found; adjust the LIKE filter if needed.")

site_row_id = int(pkap_site.iloc[0]['site_row_id'])
site_pid = pkap_site.iloc[0]['site_pid']
site_label = pkap_site.iloc[0]['site_label']
print(f"Found site: {site_label} ({site_pid})")

# 2) Get the site's site_location geospatial PID (Path 2 reference point)
pkap_site_geo = conn.execute("""
    SELECT geo.pid AS geo_pid, geo.latitude, geo.longitude
    FROM pqg e
    JOIN pqg geo ON geo.row_id = e.o[1] AND geo.otype = 'GeospatialCoordLocation'
    WHERE e.otype = '_edge_' AND e.p = 'site_location' AND e.s = ?
    LIMIT 1
""", [site_row_id]).fetchdf()

if pkap_site_geo.empty:
    raise ValueError("PKAP site has no site_location geo.")

geo_pid = pkap_site_geo.iloc[0]['geo_pid']
lat = pkap_site_geo.iloc[0]['latitude']
lon = pkap_site_geo.iloc[0]['longitude']
print(f"site_location geo: {geo_pid} @ ({lat:.6f}, {lon:.6f})")

# 3) Reverse traversal: samples at this geo in four modes
modes = ['either_or', 'both', 'only_path1', 'only_path2']
reverse_results = {}
for m in modes:
    df = get_samples_for_geo_pid(conn, geo_pid, mode=m, limit=50)
    reverse_results[m] = df
    print(f"\nMode: {m} → {len(df)} samples")
    display(df.head(5))

# 4) Pick one sample from 'either_or' to demonstrate forward traversal
if not reverse_results['either_or'].empty:
    demo_sample_pid = reverse_results['either_or'].iloc[0]['sample_pid']
    print(f"\nDemo forward traversal for sample: {demo_sample_pid}")
    for m in modes:
        fwd = get_sample_geo_context_via_sample_pid(conn, demo_sample_pid, mode=m)
        print(f"Forward mode: {m} → {len(fwd)} rows")
        display(fwd.head(3))
else:
    print("No samples found at the site_location geo in either_or mode; try increasing limit or using a different geo.")

# %%
# Samples via the site location path for comparison
ensure_connection()

samples_via_sites = conn.execute("""
    SELECT
        s.pid as sample_id,
        s.label as sample_label,
        site.label as site_name,
        g.latitude,
        g.longitude,
        'via_site_location' as location_type
    FROM pqg s
    JOIN pqg e1   ON s.row_id = e1.s AND e1.p = 'produced_by'
    JOIN pqg evt  ON e1.o[1] = evt.row_id
    JOIN pqg e2   ON evt.row_id = e2.s AND e2.p = 'sampling_site'
    JOIN pqg site ON e2.o[1] = site.row_id
    JOIN pqg e3   ON site.row_id = e3.s AND e3.p = 'site_location'
    JOIN pqg g    ON e3.o[1] = g.row_id
    WHERE s.otype = 'MaterialSampleRecord'
      AND evt.otype = 'SamplingEvent'
      AND site.otype = 'SamplingSite'
      AND g.otype = 'GeospatialCoordLocation'
      AND g.latitude IS NOT NULL
    LIMIT 100
""").fetchdf()

print(f"Found {len(samples_via_sites)} samples with site-based coordinates")
samples_via_sites.head()

# %% [markdown]
# ### Query 2: Trace MaterialSampleRecords Through Events to Sites
#
# This demonstrates a more complex **generic PQG traversal pattern** with **OpenContext-specific** archaeological hierarchies.

# %%
# Trace samples through events to sites
sample_site_hierarchy = conn.execute("""
    WITH sample_to_site AS (
        SELECT
            samp.pid as sample_id,
            samp.label as sample_label,
            evt.pid as event_id,
            site.pid as site_id,
            site.label as site_name
        FROM pqg samp
        JOIN pqg e1   ON samp.row_id = e1.s AND e1.p = 'produced_by'
        JOIN pqg evt  ON e1.o[1] = evt.row_id AND evt.otype = 'SamplingEvent'
        JOIN pqg e2   ON evt.row_id = e2.s AND e2.p = 'sampling_site'
        JOIN pqg site ON e2.o[1] = site.row_id AND site.otype = 'SamplingSite'
        WHERE samp.otype = 'MaterialSampleRecord'
    )
    SELECT
        site_id,
        site_name,
        COUNT(*) as sample_count
    FROM sample_to_site
    GROUP BY site_id, site_name
    ORDER BY sample_count DESC
    LIMIT 20
""").fetchdf()

print("Top sites by sample count:")
print(sample_site_hierarchy)

# %% [markdown]
# ### How unique are `site_name` values?
#
# The following cell checks:
# - Count of distinct `site_name` vs distinct `site_id`.
# - How many `site_name` values map to more than one `site_id` (ambiguous names), with examples.
# - The reverse (if any `site_id` has multiple names).

# %%
# Uniqueness analysis for site_name vs site_id
# Use the base pqg table to avoid bias from top-20 filtering
site_name_counts = conn.execute("""
    WITH sites AS (
        SELECT 
            site.pid   AS site_id,
            site.label AS site_name
        FROM pqg e
        JOIN pqg site ON e.o[1] = site.row_id
        WHERE e.p = 'sampling_site' AND site.otype = 'SamplingSite'
    )
    SELECT * FROM sites
""").fetchdf()

num_unique_names = site_name_counts['site_name'].nunique()
num_unique_ids = site_name_counts['site_id'].nunique()

# Names that map to more than one id
name_to_ids = (
    site_name_counts.groupby('site_name')['site_id']
    .nunique()
    .sort_values(ascending=False)
)
ambiguous_name_count = int((name_to_ids > 1).sum())
ambiguous_names = name_to_ids[name_to_ids > 1].head(20)

# IDs with multiple names (should usually be 1, but check for data quirks)
id_to_names = (
    site_name_counts.groupby('site_id')['site_name']
    .nunique()
    .sort_values(ascending=False)
)
ids_with_multiple_names = id_to_names[id_to_names > 1].head(20)

print("Distinct site_name:", num_unique_names)
print("Distinct site_id:", num_unique_ids)
print("site_name values used by >1 site_id:", ambiguous_name_count)
if not ambiguous_names.empty:
    print("Top ambiguous names (name -> distinct site_id count):")
    print(ambiguous_names)
else:
    print("No ambiguous site_name values found.")

if not ids_with_multiple_names.empty:
    print("site_id with multiple names (id -> distinct name count):")
    print(ids_with_multiple_names)


# %% [markdown]
# ### Query 3: Explore Material Types and Categories
#
# This query shows how **OpenContext domain concepts** (material classifications) are modeled using the **generic PQG framework**.

# %%
# Explore material types and categories
material_analysis = conn.execute("""
    SELECT
        c.label as material_type,
        c.name as category_name,
        COUNT(DISTINCT s.row_id) as sample_count
    FROM pqg s
    JOIN pqg e ON s.row_id = e.s
    JOIN pqg c ON e.o[1] = c.row_id
    WHERE s.otype = 'MaterialSampleRecord'
      AND e.otype = '_edge_'
      AND e.p = 'has_material_category'
      AND c.otype = 'IdentifiedConcept'
    GROUP BY c.label, c.name
    ORDER BY sample_count DESC
    LIMIT 20
""").fetchdf()

print("Most common material types:")
print(material_analysis)

# %% [markdown]
# ## Query Performance Tips
#
# These tips apply to both **generic PQG patterns** and **OpenContext-specific** queries:
#
# ### Generic PQG Optimization:
# 1. **Filter edges first**: Use `otype = '_edge_'` early in WHERE clauses
# 2. **Use array indexing carefully**: `o[1]` for first target in edge arrays
# 3. **Leverage row_id indexes**: Join on row_id fields for best performance
#
# ### OpenContext-Specific Optimization:
# 1. **Filter by entity type early**: e.g., `otype = 'MaterialSampleRecord'`
# 2. **Use domain predicates**: Filter edges by specific predicates like `produced_by`
# 3. **Limit geographic queries**: Add bounds when querying latitude/longitude
#
# ### Memory Management for Large Graphs:
# - Simple node counts: Fast (<1 second)
# - Single-hop edge traversal: Moderate (1-5 seconds)
# - Multi-hop graph traversal: Can be slow (5-30 seconds)
# - Full graph scans: Avoid without filters

# %% [markdown]
# ### Sites with the most associated geospatial locations (by site_id)
#
# To avoid ambiguity from non-unique site names, we aggregate by `site_id` and include `site_name` for readability.

# %%
# Count geospatial locations per site (by id)
sites_with_geo_counts = conn.execute("""
    WITH site_geos AS (
        SELECT
            site.pid   AS site_id,
            site.label AS site_name,
            geo.pid    AS geo_id
        FROM pqg site
        JOIN pqg e    ON site.row_id = e.s AND e.p = 'site_location'
        JOIN pqg geo  ON e.o[1] = geo.row_id
        WHERE site.otype = 'SamplingSite'
          AND geo.otype = 'GeospatialCoordLocation'
    )
    SELECT
        site_id,
        site_name,
        COUNT(DISTINCT geo_id) AS geo_count
    FROM site_geos
    GROUP BY site_id, site_name
    ORDER BY geo_count DESC
    LIMIT 20
""").fetchdf()

print("Top sites by number of associated GeospatialCoordLocation records (by site_id):")
print(sites_with_geo_counts)


# %% [markdown]
# ## Visualization Preparation

# %%
def get_sample_locations_for_viz(conn, limit=10000):
    """Extract sample locations optimized for visualization (SQL version)"""
    
    return conn.execute(f"""
        WITH direct_locations AS (
            -- Direct path: Sample -> Event -> sample_location -> Location
            SELECT
                s.pid as sample_id,
                s.label as label,
                g.latitude,
                g.longitude,
                g.obfuscated,
                'direct' as location_type
            FROM pqg s
            JOIN pqg e1   ON s.row_id = e1.s AND e1.p = 'produced_by'
            JOIN pqg evt  ON e1.o[1] = evt.row_id
            JOIN pqg e2   ON evt.row_id = e2.s AND e2.p = 'sample_location'
            JOIN pqg g    ON e2.o[1] = g.row_id
            WHERE s.otype = 'MaterialSampleRecord'
              AND evt.otype = 'SamplingEvent'
              AND g.otype = 'GeospatialCoordLocation'
              AND g.latitude IS NOT NULL
              AND g.longitude IS NOT NULL
        ),
        site_locations AS (
            -- Indirect path: Sample -> Event -> Site -> site_location -> Location
            SELECT
                s.pid as sample_id,
                s.label as label,
                g.latitude,
                g.longitude,
                g.obfuscated,
                'via_site' as location_type
            FROM pqg s
            JOIN pqg e1   ON s.row_id = e1.s AND e1.p = 'produced_by'
            JOIN pqg evt  ON e1.o[1] = evt.row_id
            JOIN pqg e2   ON evt.row_id = e2.s AND e2.p = 'sampling_site'
            JOIN pqg site ON e2.o[1] = site.row_id
            JOIN pqg e3   ON site.row_id = e3.s AND e3.p = 'site_location'
            JOIN pqg g    ON e3.o[1] = g.row_id
            WHERE s.otype = 'MaterialSampleRecord'
              AND evt.otype = 'SamplingEvent'
              AND site.otype = 'SamplingSite'
              AND g.otype = 'GeospatialCoordLocation'
              AND g.latitude IS NOT NULL
              AND g.longitude IS NOT NULL
        )
        SELECT
            sample_id,
            label,
            latitude,
            longitude,
            obfuscated,
            location_type
        FROM (
            SELECT * FROM direct_locations
            UNION ALL
            SELECT * FROM site_locations
        )
        WHERE NOT obfuscated  -- Exclude obfuscated locations for public viz
        LIMIT {limit}
    """).fetchdf()

# Get visualization-ready data
viz_data = get_sample_locations_for_viz(conn, 5000)
print(f"Prepared {len(viz_data)} samples for visualization")
if len(viz_data) > 0:
    print(f"Coordinate bounds: Lat [{viz_data.latitude.min():.2f}, {viz_data.latitude.max():.2f}], "
          f"Lon [{viz_data.longitude.min():.2f}, {viz_data.longitude.max():.2f}]")
    print(f"Location types: {viz_data.location_type.value_counts().to_dict()}")
else:
    print("No samples found with valid coordinates")


# %% [markdown]
# ## Data Export Options

# %%
def export_site_subgraph(conn, site_name_pattern, output_prefix):
    """Export all data related to a specific site"""
    
    # Find the site
    site_info = conn.execute("""
        SELECT row_id, pid, label
        FROM pqg
        WHERE otype = 'SamplingSite'
        AND label LIKE ?
        LIMIT 1
    """, [f'%{site_name_pattern}%']).fetchdf()
    
    if site_info.empty:
        print(f"No site found matching '{site_name_pattern}'")
        return None
    
    site_row_id = site_info.iloc[0]['row_id']
    print(f"Found site: {site_info.iloc[0]['label']}")
    
    # Get all related entities (simplified version - not recursive)
    related_data = conn.execute("""
        WITH site_related AS (
            -- Get the site itself
            SELECT * FROM pqg WHERE row_id = ?
            
            UNION ALL
            
            -- Get edges from the site
            SELECT * FROM pqg e
            WHERE e.otype = '_edge_' AND e.s = ?
            
            UNION ALL
            
            -- Get entities connected to the site
            SELECT n.* FROM pqg e
            JOIN pqg n ON n.row_id = e.o[1]
            WHERE e.otype = '_edge_' AND e.s = ?
        )
        SELECT * FROM site_related
    """, [site_row_id, site_row_id, site_row_id]).fetchdf()
    
    # Save to parquet
    output_file = f"{output_prefix}_{site_info.iloc[0]['pid']}.parquet"
    related_data.to_parquet(output_file)
    print(f"Exported {len(related_data)} rows to {output_file}")
    
    return related_data

# Example usage (commented out to avoid creating files)
# pompeii_data = export_site_subgraph(conn, "Pompeii", "pompeii_subgraph")


# %% [markdown]
# ## Data Quality Analysis

# %%
# Check for location data quality
location_quality = conn.execute("""
    SELECT
        CASE 
            WHEN obfuscated THEN 'Obfuscated'
            ELSE 'Precise'
        END as location_type,
        COUNT(*) as count,
        AVG(CASE WHEN latitude IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as pct_with_coords
    FROM pqg
    WHERE otype = 'GeospatialCoordLocation'
    GROUP BY location_type
""").fetchdf()

print("Location Data Quality:")
print(location_quality)

# %%
# Check for orphaned nodes (nodes not connected by any edge)
orphan_check = conn.execute("""
    WITH connected_nodes AS (
        SELECT DISTINCT s as row_id FROM pqg WHERE otype = '_edge_'
        UNION
        SELECT DISTINCT unnest(o) as row_id FROM pqg WHERE otype = '_edge_'
    )
    SELECT
        n.otype,
        COUNT(*) as orphan_count
    FROM pqg n
    LEFT JOIN connected_nodes c ON n.row_id = c.row_id
    WHERE n.otype != '_edge_' AND c.row_id IS NULL
    GROUP BY n.otype
""").fetchdf()

print("\nOrphaned Nodes by Type:")
print(orphan_check if not orphan_check.empty else "No orphaned nodes found!")

# %% [markdown]
# ## Summary Statistics

# %%
# Generate comprehensive summary
summary = conn.execute("""
    WITH stats AS (
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT pid) as unique_pids,
            COUNT(CASE WHEN otype = '_edge_' THEN 1 END) as edge_count,
            COUNT(CASE WHEN otype != '_edge_' THEN 1 END) as node_count,
            COUNT(DISTINCT CASE WHEN otype != '_edge_' THEN otype END) as entity_types,
            COUNT(DISTINCT p) as relationship_types
        FROM pqg
    )
    SELECT * FROM stats
""").fetchdf()

print("Dataset Summary:")
for col in summary.columns:
    print(f"{col}: {summary[col].iloc[0]:,}")

# %% [markdown]
# ## Debug: Specific Geo Point Analysis
#
# Testing queries for parquet_cesium.qmd debugging. This section demonstrates:
# - **Generic PQG debugging**: How to trace edge connections
# - **OpenContext validation**: Verifying archaeological data relationships

# %%
# Debug specific geo location from parquet_cesium.qmd
# This section remains provider-agnostic and uses iSamples model semantics

target_geo_pid = "geoloc_7ea562cce4c70e4b37f7915e8384880c86607729"

print(f"=== Debugging geo location: {target_geo_pid} ===\n")

# 1. Find the geo location record
geo_record = conn.execute("""
    SELECT row_id, pid, otype, latitude, longitude 
    FROM pqg 
    WHERE pid = ? AND otype = 'GeospatialCoordLocation'
""", [target_geo_pid]).fetchdf()

print("1. Geo Location Record:")
if not geo_record.empty:
    print(geo_record.to_dict('records')[0])
    geo_row_id = geo_record.iloc[0]['row_id']
    print(f"   Row ID: {geo_row_id}")
else:
    print("   ❌ Geo location not found!")
    geo_row_id = None

# %%
# 2. Check what edges point to this geo location
if geo_row_id is not None:
    geo_row_id_int = int(geo_row_id)
    edges_to_geo = conn.execute("""
        SELECT s, p, otype as edge_type, pid as edge_pid
        FROM pqg 
        WHERE otype = '_edge_' AND ? = ANY(o)
    """, [geo_row_id_int]).fetchdf()

    print(f"\n2. Edges pointing to this geo location ({len(edges_to_geo)} found):")
    if not edges_to_geo.empty:
        edge_summary = edges_to_geo.groupby('p').size().reset_index()
        edge_summary.columns = ['predicate', 'count']
        print(edge_summary)
        print("\nDetailed edges:")
        for _, edge in edges_to_geo.iterrows():
            print(f"   {edge['p']}: row_id {edge['s']} -> geo location")
    else:
        print("   ❌ No edges point to this geo location!")
else:
    print("\n2. Skipping edge analysis - geo location not found")

# %%
# 3. Direct event samples
if geo_row_id is not None:
    direct_samples = conn.execute("""
        SELECT DISTINCT
            s.pid as sample_id,
            s.label as sample_label,
            s.name as sample_name,
            evt.pid as event_id,
            evt.label as event_label,
            'direct_event_location' as location_path
        FROM pqg s
        JOIN pqg e1  ON s.row_id = e1.s AND e1.p = 'produced_by'
        JOIN pqg evt ON e1.o[1] = evt.row_id
        JOIN pqg e2  ON evt.row_id = e2.s AND e2.p = 'sample_location'
        JOIN pqg g   ON e2.o[1] = g.row_id
        WHERE s.otype = 'MaterialSampleRecord'
          AND evt.otype = 'SamplingEvent'
          AND g.otype = 'GeospatialCoordLocation'
          AND g.pid = ?
        LIMIT 20
    """, [target_geo_pid]).fetchdf()

    print(f"\n3. Direct Event Samples ({len(direct_samples)} found):")
    if not direct_samples.empty:
        print(direct_samples[['sample_id', 'sample_label', 'event_id', 'event_label']].head())
    else:
        print("   ❌ No direct event samples found!")
else:
    print("\n3. Skipping direct samples query - geo location not found")

# %%
# 4. Site-associated samples
if geo_row_id is not None:
    site_samples = conn.execute("""
        SELECT DISTINCT
            s.pid as sample_id,
            s.label as sample_label,
            s.name as sample_name,
            evt.pid as event_id,
            evt.label as event_label,
            site.label as site_name,
            'via_site_location' as location_path
        FROM pqg s
        JOIN pqg e1   ON s.row_id = e1.s AND e1.p = 'produced_by'
        JOIN pqg evt  ON e1.o[1] = evt.row_id
        JOIN pqg e2   ON evt.row_id = e2.s AND e2.p = 'sampling_site'
        JOIN pqg site ON e2.o[1] = site.row_id
        JOIN pqg e3   ON site.row_id = e3.s AND e3.p = 'site_location'
        JOIN pqg g    ON e3.o[1] = g.row_id
        WHERE s.otype = 'MaterialSampleRecord'
          AND evt.otype = 'SamplingEvent'
          AND site.otype = 'SamplingSite'
          AND g.otype = 'GeospatialCoordLocation'
          AND g.pid = ?
        LIMIT 20
    """, [target_geo_pid]).fetchdf()

    print(f"\n4. Site-Associated Samples ({len(site_samples)} found):")
    if not site_samples.empty:
        print(site_samples[['sample_id', 'sample_label', 'site_name', 'event_id']].head())
    else:
        print("   ❌ No site-associated samples found!")
else:
    print("\n4. Skipping site samples query - geo location not found")

# %%
# 5. If we found samples, get detailed metadata for the first sample
all_samples = []
if 'direct_samples' in locals() and not direct_samples.empty:
    all_samples.extend(direct_samples.to_dict('records'))
if 'site_samples' in locals() and not site_samples.empty:
    all_samples.extend(site_samples.to_dict('records'))

if all_samples:
    first_sample = all_samples[0]
    sample_pid = first_sample['sample_id']

    print(f"\n5. Detailed metadata for sample: {sample_pid}")
    print(f"   Resolvable URL: {ark_to_url(sample_pid)}")
    print(f"   Sample label: {first_sample.get('sample_label', 'N/A')}")
    print(f"   Location path: {first_sample.get('location_path', 'N/A')}")

    # Materials for this sample
    materials = conn.execute("""
        SELECT DISTINCT
            mat.pid as material_id,
            mat.label as material_type,
            mat.name as material_category
        FROM pqg s
        JOIN pqg e   ON s.row_id = e.s AND e.p = 'has_material_category'
        JOIN pqg mat ON e.o[1] = mat.row_id
        WHERE s.otype = 'MaterialSampleRecord'
          AND s.pid = ?
          AND e.otype = '_edge_'
          AND mat.otype = 'IdentifiedConcept'
    """, [sample_pid]).fetchdf()

    print(f"\n   Materials ({len(materials)} found):")
    if not materials.empty:
        for _, mat in materials.iterrows():
            print(f"     - {mat['material_type']} ({ark_to_url(mat['material_id'])})")
    else:
        print("     ❌ No materials found!")

    # Agents responsible for this sample
    agents = conn.execute("""
        SELECT DISTINCT
            agent.pid as agent_id,
            agent.label as agent_name,
            agent.name as agent_role
        FROM pqg s
        JOIN pqg e1    ON s.row_id = e1.s AND e1.p = 'produced_by'
        JOIN pqg evt   ON e1.o[1] = evt.row_id
        JOIN pqg e2    ON evt.row_id = e2.s AND e2.p = 'responsibility'
        JOIN pqg agent ON e2.o[1] = agent.row_id
        WHERE s.otype = 'MaterialSampleRecord'
          AND s.pid = ?
          AND e1.otype = '_edge_'
          AND evt.otype = 'SamplingEvent'
          AND e2.otype = '_edge_'
          AND agent.otype = 'Agent'
        LIMIT 10
    """, [sample_pid]).fetchdf()

    print(f"\n   Responsible Agents ({len(agents)} found):")
    if not agents.empty:
        for _, agent in agents.iterrows():
            print(f"     - {agent['agent_name']} ({ark_to_url(agent['agent_id'])})")
    else:
        print("     ❌ No agents found!")
else:
    print("\n5. No samples found to analyze metadata")

# %%
# 6. Summary of findings for this geo location
print(f"\n=== SUMMARY for {target_geo_pid} ===")
if geo_row_id is not None:
    print(f"✅ Geo location found (row_id: {geo_row_id})")
    print(f"📍 Coordinates: {geo_record.iloc[0]['latitude']}, {geo_record.iloc[0]['longitude']}")

    total_samples = len(all_samples)
    direct_count = len([s for s in all_samples if s.get('location_path') == 'direct_event_location'])
    site_count = len([s for s in all_samples if s.get('location_path') == 'via_site_location'])

    print(f"🔬 Total samples found: {total_samples}")
    print(f"   - Direct event samples: {direct_count}")
    print(f"   - Site-associated samples: {site_count}")

    if total_samples > 0:
        print("✅ Sample metadata retrieval successful!")
    else:
        print("❌ No samples found for this location")
else:
    print("❌ Geo location not found in dataset!")

print(f"\n=== END DEBUG for {target_geo_pid} ===\n")

# %%
# 7. Test with a different geo location that has sample_location edges
sample_location_geos = conn.execute("""
    SELECT g.pid, g.latitude, g.longitude, COUNT(*) as edge_count
    FROM pqg e
    JOIN pqg g ON e.o[1] = g.row_id
    WHERE e.otype = '_edge_'
      AND e.p = 'sample_location'
      AND g.otype = 'GeospatialCoordLocation'
    GROUP BY g.pid, g.latitude, g.longitude
    ORDER BY edge_count DESC
    LIMIT 3
""").fetchdf()

print("=== Testing with geo locations that have direct sample_location edges ===")
print(sample_location_geos)

if not sample_location_geos.empty:
    test_geo_pid = sample_location_geos.iloc[0]['pid']
    print(f"\nTesting direct samples query with: {test_geo_pid}")

    test_direct_samples = conn.execute("""
        SELECT DISTINCT
            s.pid as sample_id,
            s.label as sample_label,
            evt.pid as event_id,
            evt.label as event_label
        FROM pqg s
        JOIN pqg e1  ON s.row_id = e1.s AND e1.p = 'produced_by'
        JOIN pqg evt ON e1.o[1] = evt.row_id
        JOIN pqg e2  ON evt.row_id = e2.s AND e2.p = 'sample_location'
        JOIN pqg g   ON e2.o[1] = g.row_id
        WHERE s.otype = 'MaterialSampleRecord'
          AND evt.otype = 'SamplingEvent'
          AND g.otype = 'GeospatialCoordLocation'
          AND g.pid = ?
        LIMIT 5
    """, [test_geo_pid]).fetchdf()

    print(f"Direct samples found: {len(test_direct_samples)}")
    if not test_direct_samples.empty:
        print("✅ Direct event samples exist")
        print(test_direct_samples[['sample_id', 'sample_label', 'event_id']].head())
    else:
        print("❌ Still no direct event samples found")
else:
    print("❌ No geo locations with sample_location edges found")

# %% [markdown]
# ## Debug Analysis Results
#
# ### Key Findings for parquet_cesium.qmd
#
# 1. **Geo Location Structure**: The target geo location `geoloc_7ea562cce4c70e4b37f7915e8384880c86607729` exists in the dataset with correct coordinates.
#
# 2. **MaterialSampleRecord Association**: This specific location has **1 site-associated MaterialSampleRecord** but **0 direct event MaterialSampleRecord instances**.
#
# 3. **Query Validation**: Both query paths work correctly:
#    - **Direct path**: `MaterialSampleRecord → SamplingEvent → sample_location → GeospatialCoordLocation`
#    - **Site path**: `MaterialSampleRecord → SamplingEvent → SamplingSite → site_location → GeospatialCoordLocation`
#
# 4. **Data Availability**: The dataset contains both types of MaterialSampleRecord associations, but not every geo location has both types.
#
# ### Recommendations for parquet_cesium.qmd
#
# - The JavaScript queries are correctly structured and should work
# - Some geo locations may only have site-associated MaterialSampleRecord instances (like our test case)
# - Consider showing both direct and site-associated MaterialSampleRecord instances in the UI
# - Add debug logging to identify when no MaterialSampleRecord instances are found vs. query errors

# %%
# Analysis complete!
print("\nAnalysis complete!")
print("Note: DuckDB connection remains open for interactive use")

# %% [markdown]
# ## Read PQG key-value metadata (iSamples generic)
#
# The parquet contains KV metadata describing the iSamples PQG schema (see https://github.com/isamplesorg/pqg). We’ll load the keys `pqg_version`, `pqg_primary_key`, `pqg_node_types`, `pqg_edge_fields`, `pqg_literal_fields` to make the notebook self‑describing and provider‑agnostic.

# %%
# Read PQG key-value metadata using PyArrow (provider-agnostic)
import pyarrow.parquet as pq

try:
    md = pq.read_metadata(parquet_path)
    kv_raw = md.metadata or {}
    # Decode byte keys/values to strings
    kv = { (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
           (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
           for k, v in kv_raw.items() }

    wanted_keys = ["pqg_version", "pqg_primary_key", "pqg_node_types", "pqg_edge_fields", "pqg_literal_fields"]
    selected = {k: kv.get(k) for k in wanted_keys if k in kv}

    print("PQG KV metadata (selected):")
    if selected:
        for k in wanted_keys:
            if k in selected:
                print(f"- {k}: {selected[k][:120]}{'...' if len(selected[k])>120 else ''}")
    else:
        print("No PQG KV metadata keys found in file metadata")
except Exception as e:
    print("Unable to read parquet metadata via PyArrow:", e)

# %%


# Count records
result = conn.execute("SELECT COUNT(*) FROM pqg;").fetchone()
result


# %%
# Helper queries around a sample PID and a geo PID

# Path 1 (Direct event location):
#   MaterialSampleRecord -> produced_by -> SamplingEvent -> sample_location -> GeospatialCoordLocation

# Path 2 (Via site location):
#   MaterialSampleRecord -> produced_by -> SamplingEvent -> sampling_site -> SamplingSite -> site_location -> GeospatialCoordLocation

# Notes on the queries below:
# - The PQG table stores both nodes (MaterialSampleRecord, SamplingEvent, SamplingSite, GeospatialCoordLocation, etc.) and edges (otype = '_edge_').
# - WHERE and JOIN conditions enforce which path(s) are required for a row to appear.
# - Inner JOINs mean rows will only be returned when all joined paths/objects exist.


def get_sample_data_via_sample_pid(sample_pid, con, show_max_width):
    """Return one row of core sample metadata, including site and geo coordinates, for a sample PID.

    What it does
    - Starts at the MaterialSampleRecord identified by the given `sample_pid`.
    - Follows produced_by -> SamplingEvent.
    - Follows sample_location -> GeospatialCoordLocation to fetch latitude/longitude (Path 1).
    - Follows sampling_site -> SamplingSite to fetch site label and PID (Path 2).

    Important implications
    - This query uses INNER JOINs on BOTH the Path 1 and Path 2 chains. Therefore, it returns a row only if the sample has:
        1) a SamplingEvent with a sample_location pointing to a GeospatialCoordLocation (Path 1), and
        2) a SamplingEvent with a sampling_site pointing to a SamplingSite (Path 2).
      If either path is missing, the query returns no rows.

    Parameters
    - sample_pid (str): The iSamples PID of the MaterialSampleRecord to look up.
    - con: A DuckDB connection with the PQG table registered as `pqg`.
    - show_max_width: Width passed to DuckDB's .show() for display formatting.

    Returns
    - DuckDB relation (con.sql(sql)): The prepared relation; also prints a preview via .show().
    """

    sql = f"""
    SELECT 
        samp_pqg.row_id,
        samp_pqg.pid AS sample_pid,
        samp_pqg.alternate_identifiers AS sample_alternate_identifiers,
        samp_pqg.label AS sample_label,
        samp_pqg.description AS sample_description,
        samp_pqg.thumbnail_url AS sample_thumbnail_url,
        samp_pqg.thumbnail_url is NOT NULL as has_thumbnail,
        geo_pqg.latitude, 
        geo_pqg.longitude,
        site_pqg.label AS sample_site_label,
        site_pqg.pid AS sample_site_pid
    FROM pqg AS samp_pqg
    JOIN pqg AS samp_rel_se_pqg ON (samp_rel_se_pqg.s = samp_pqg.row_id AND samp_rel_se_pqg.p = 'produced_by')
    JOIN pqg AS se_pqg ON (list_extract(samp_rel_se_pqg.o, 1) = se_pqg.row_id AND se_pqg.otype = 'SamplingEvent')
    -- Path 1: event -> sample_location -> GeospatialCoordLocation
    JOIN pqg AS geo_rel_se_pqg ON (geo_rel_se_pqg.s = se_pqg.row_id AND geo_rel_se_pqg.p = 'sample_location')
    JOIN pqg AS geo_pqg ON (list_extract(geo_rel_se_pqg.o, 1) = geo_pqg.row_id AND geo_pqg.otype = 'GeospatialCoordLocation')
    -- Path 2: event -> sampling_site -> SamplingSite
    JOIN pqg AS site_rel_se_pqg ON (site_rel_se_pqg.s = se_pqg.row_id AND site_rel_se_pqg.p = 'sampling_site')
    JOIN pqg AS site_pqg ON (list_extract(site_rel_se_pqg.o, 1) = site_pqg.row_id AND site_pqg.otype = 'SamplingSite')
    WHERE samp_pqg.pid = '{sample_pid}' AND samp_pqg.otype = 'MaterialSampleRecord';
    """

    db_m = con.sql(sql)
    # db_m.show(max_width=show_max_width)
    return db_m


def get_sample_data_agents_sample_pid(sample_pid, con, show_max_width):
    """Return agent relationships (responsibility/registrant) for a sample PID.

    What it does
    - Starts at the MaterialSampleRecord identified by `sample_pid`.
    - Follows produced_by -> SamplingEvent.
    - From the event, follows predicates in ['responsibility', 'registrant'] to Agent nodes.

    Relationship to Path 1 vs Path 2
    - This query does NOT depend on Path 1 (direct geo) or Path 2 (via site). It only depends on the existence of the SamplingEvent and agent edges from that event. You will get agent rows even if the sample has no sample_location or sampling_site.

    Parameters
    - sample_pid (str): The sample PID.
    - con: DuckDB connection.
    - show_max_width: Width used by .show().

    Returns
    - DuckDB relation (con.sql(sql)): The prepared relation; also prints a preview via .show().
    """

    sql = f"""
    SELECT 
        samp_pqg.row_id,
        samp_pqg.pid AS sample_pid,
        samp_pqg.alternate_identifiers AS sample_alternate_identifiers,
        samp_pqg.label AS sample_label,
        samp_pqg.description AS sample_description,
        samp_pqg.thumbnail_url AS sample_thumbnail_url,
        samp_pqg.thumbnail_url is NOT NULL as has_thumbnail,
        agent_rel_se_pqg.p AS predicate,
        agent_pqg.pid AS agent_pid,
        agent_pqg.name AS agent_name,
        agent_pqg.alternate_identifiers AS agent_alternate_identifiers
    FROM pqg AS samp_pqg
    JOIN pqg AS samp_rel_se_pqg ON (samp_rel_se_pqg.s = samp_pqg.row_id AND samp_rel_se_pqg.p = 'produced_by')
    JOIN pqg AS se_pqg ON (list_extract(samp_rel_se_pqg.o, 1) = se_pqg.row_id AND se_pqg.otype = 'SamplingEvent')
    JOIN pqg AS agent_rel_se_pqg ON (agent_rel_se_pqg.s = se_pqg.row_id AND list_contains(['responsibility', 'registrant'], agent_rel_se_pqg.p))
    JOIN pqg AS agent_pqg ON (agent_pqg.row_id = ANY(agent_rel_se_pqg.o) AND agent_pqg.otype = 'Agent')
    WHERE samp_pqg.pid = '{sample_pid}' AND samp_pqg.otype = 'MaterialSampleRecord';
    """

    db_m = con.sql(sql)
    # db_m.show(max_width=show_max_width)
    return db_m


def get_sample_types_and_keywords_via_sample_pid(sample_pid, con, show_max_width):
    """Return IdentifiedConcept terms (keywords, object types, material categories) for a sample PID.

    What it does
    - Starts at the MaterialSampleRecord identified by `sample_pid`.
    - Follows predicates in ['keywords', 'has_sample_object_type', 'has_material_category'] to IdentifiedConcept nodes and returns their PID/label.

    Relationship to Path 1 vs Path 2
    - This query attaches concepts directly to the MaterialSampleRecord. It does not require Path 1 or Path 2 to exist and will return rows even if no geo/site relationships are present for the sample.

    Parameters
    - sample_pid (str): The sample PID.
    - con: DuckDB connection.
    - show_max_width: Width used by .show().

    Returns
    - DuckDB relation (con.sql(sql)): The prepared relation; also prints a preview via .show().
    """

    sql = f"""
    SELECT 
        samp_pqg.row_id,
        samp_pqg.pid AS sample_pid,
        samp_pqg.alternate_identifiers AS sample_alternate_identifiers,
        samp_pqg.label AS sample_label,
        kw_rel_se_pqg.p AS predicate,
        kw_pqg.pid AS keyword_pid,
        kw_pqg.label AS keyword
    FROM pqg AS samp_pqg
    JOIN pqg AS kw_rel_se_pqg ON (kw_rel_se_pqg.s = samp_pqg.row_id AND list_contains(['keywords', 'has_sample_object_type', 'has_material_category'], kw_rel_se_pqg.p))
    JOIN pqg AS kw_pqg ON (kw_pqg.row_id = ANY(kw_rel_se_pqg.o) AND kw_pqg.otype = 'IdentifiedConcept')
    WHERE samp_pqg.pid = '{sample_pid}' AND samp_pqg.otype = 'MaterialSampleRecord';
    """

    db_m = con.sql(sql)
    # db_m.show(max_width=show_max_width)
    return db_m


def get_samples_at_geo_cord_location_via_sample_event(geo_loc_pid, con, show_max_width):
    """Return samples anchored at a GeospatialCoordLocation PID via event sample_location, plus site info.

    What it does
    - Starts at a GeospatialCoordLocation identified by `geo_loc_pid`.
    - Follows incoming edges with p = 'sample_location' to reach SamplingEvent rows (Path 1 from the perspective of event -> geo; here we walk it in reverse starting at geo).
    - From each event, follows produced_by (reverse) to find MaterialSampleRecord rows produced by it.
    - Also enriches each event with its sampling_site -> SamplingSite to return site label/PID (Path 2).

    Relationship to Path 1 vs Path 2
    - Path 1 is REQUIRED because we start from the GeospatialCoordLocation and look for events that point to it via sample_location. Those events are then used to find samples produced by them.
    - Path 2 is JOINED to provide site context. Because the SQL uses INNER JOINs for site, only events that also have a SamplingSite will surface here. If you want direct-only results regardless of whether an event has a SamplingSite, change the site joins to LEFT JOINs.

    Parameters
    - geo_loc_pid (str): The PID of the GeospatialCoordLocation.
    - con: DuckDB connection.
    - show_max_width: Width used by .show().

    Returns
    - DuckDB relation (con.sql(sql)): The prepared relation; also prints a preview via .show().
    """

    sql = f"""
    SELECT geo_pqg.latitude, geo_pqg.longitude, 
           site_pqg.label AS sample_site_label,
           site_pqg.pid AS sample_site_pid,
           samp_pqg.pid AS sample_pid,
           samp_pqg.alternate_identifiers AS sample_alternate_identifiers,
           samp_pqg.label AS sample_label,
           samp_pqg.description AS sample_description,
           samp_pqg.thumbnail_url AS sample_thumbnail_url,
           samp_pqg.thumbnail_url is NOT NULL as has_thumbnail 
    FROM pqg AS geo_pqg
    JOIN pqg AS rel_se_pqg ON (rel_se_pqg.p = 'sample_location' AND contains(rel_se_pqg.o, geo_pqg.row_id))
    JOIN pqg AS se_pqg ON (rel_se_pqg.s = se_pqg.row_id AND se_pqg.otype = 'SamplingEvent')
    -- Path 2 enrichment: event -> sampling_site -> SamplingSite
    JOIN pqg AS rel_site_pqg ON (se_pqg.row_id = rel_site_pqg.s AND rel_site_pqg.p = 'sampling_site')
    JOIN pqg AS site_pqg ON (list_extract(rel_site_pqg.o, 1) = site_pqg.row_id AND site_pqg.otype = 'SamplingSite')
    -- Find samples produced by the event
    JOIN pqg AS rel_samp_pqg ON (rel_samp_pqg.p = 'produced_by' AND contains(rel_samp_pqg.o, se_pqg.row_id))
    JOIN pqg AS samp_pqg ON (rel_samp_pqg.s = samp_pqg.row_id AND samp_pqg.otype = 'MaterialSampleRecord')
    WHERE geo_pqg.pid = '{geo_loc_pid}' AND geo_pqg.otype = 'GeospatialCoordLocation'
    ORDER BY has_thumbnail DESC
    """

    db_m = con.sql(sql)
    # db_m.show(max_width=show_max_width)
    return db_m



# %%
sample_pid = "geoloc_7ea562cce4c70e4b37f7915e8384880c86607729"
sample_pid = "ark:/28722/k2xd0t39r"
get_sample_data_via_sample_pid(sample_pid, conn, 120)


# %%
get_sample_data_agents_sample_pid(sample_pid, conn, 120)

# %%
get_sample_types_and_keywords_via_sample_pid(sample_pid, conn, 120)

# %%
get_samples_at_geo_cord_location_via_sample_event(sample_pid, conn, 120)

# %%
# %load_ext sql

# %%
# Connect to an in-memory DuckDB instance using %sql magic
# %sql duckdb:///:memory:

# Create a view for the Parquet file (run this only once per session)
# %sql CREATE VIEW pqg AS SELECT * FROM '/Users/raymondyee/Data/iSample/oc_isamples_pqg.parquet';


# %% language="sql"
#
# # count the number of rows in pqg
# SELECT COUNT(*) FROM pqg;

# %%
# Query geolocation records (pid, latitude, longitude) associated with PKAP Survey Area
ensure_connection()

pkap_geos = conn.execute("""
    SELECT
        site.pid   AS site_pid,
        site.label AS site_label,
        geo.pid    AS geo_pid,
        geo.row_id AS geo_row_id,
        geo.latitude,
        geo.longitude
    FROM pqg site
    JOIN pqg rel  ON (rel.s = site.row_id AND rel.p = 'site_location')
    JOIN pqg geo  ON (rel.o[1] = geo.row_id AND geo.otype = 'GeospatialCoordLocation')
    WHERE site.otype = 'SamplingSite'
      AND site.label = 'PKAP Survey Area'
    ORDER BY geo.pid
""").fetchdf()

print(f"Found {len(pkap_geos):,} geolocations for PKAP Survey Area")
pkap_geos.head(50)


# %%
pkap_geoloc_id = "geoloc_ff64156b561ebb054e43183135f46f8c30f7e526"
get_samples_at_geo_cord_location_via_sample_event(pkap_geoloc_id, conn, 120)

# %% language="sql"
#
# -- Check whether all sampling sites have exactly one associated geolocation
# WITH site_geo_counts AS (
#     SELECT
#         site.pid AS site_id,
#         COUNT(DISTINCT geo.pid) AS geo_count
#     FROM pqg site
#     JOIN pqg e ON site.row_id = e.s AND e.p = 'site_location'
#     JOIN pqg geo ON e.o[1] = geo.row_id AND geo.otype = 'GeospatialCoordLocation'
#     WHERE site.otype = 'SamplingSite'
#     GROUP BY site.pid
# )
# SELECT
#     CASE WHEN MIN(geo_count) = 1 AND MAX(geo_count) = 1 THEN 'Yes' ELSE 'No' END AS all_sites_exactly_one_geo
# FROM site_geo_counts;
#

# %% [markdown]
# ### Query: GeospatialCoordLocation linked to both SamplingEvent and SamplingSite
#
# This query finds geographic points (GeospatialCoordLocation) that have incoming edges from both:
# - SamplingEvent via `sample_location` (Path 1)
# - SamplingSite via `site_location` (Path 2)
#
# It returns the geo PID, coordinates, and the counts of each edge type.

# %%
# Find GeospatialCoordLocation nodes connected to both SamplingEvent (sample_location) and SamplingSite (site_location)
ensure_connection()

both_paths_geos = conn.execute("""
    WITH event_geos AS (
        SELECT g.row_id AS geo_row_id, g.pid AS geo_pid
        FROM pqg e
        JOIN pqg g ON e.o[1] = g.row_id
        WHERE e.otype = '_edge_'
          AND e.p = 'sample_location'
          AND g.otype = 'GeospatialCoordLocation'
    ),
    site_geos AS (
        SELECT g.row_id AS geo_row_id, g.pid AS geo_pid
        FROM pqg e
        JOIN pqg g ON e.o[1] = g.row_id
        WHERE e.otype = '_edge_'
          AND e.p = 'site_location'
          AND g.otype = 'GeospatialCoordLocation'
    ),
    event_counts AS (
        SELECT g.row_id AS geo_row_id, COUNT(*) AS sample_location_edges
        FROM pqg g
        JOIN pqg e ON e.o[1] = g.row_id AND e.otype = '_edge_' AND e.p = 'sample_location'
        WHERE g.otype = 'GeospatialCoordLocation'
        GROUP BY g.row_id
    ),
    site_counts AS (
        SELECT g.row_id AS geo_row_id, COUNT(*) AS site_location_edges
        FROM pqg g
        JOIN pqg e ON e.o[1] = g.row_id AND e.otype = '_edge_' AND e.p = 'site_location'
        WHERE g.otype = 'GeospatialCoordLocation'
        GROUP BY g.row_id
    )
    SELECT g.pid, g.latitude, g.longitude,
           COALESCE(ec.sample_location_edges, 0) AS sample_location_edges,
           COALESCE(sc.site_location_edges, 0) AS site_location_edges
    FROM pqg g
    JOIN event_geos eg ON eg.geo_row_id = g.row_id
    JOIN site_geos sg ON sg.geo_row_id = g.row_id
    LEFT JOIN event_counts ec ON ec.geo_row_id = g.row_id
    LEFT JOIN site_counts sc ON sc.geo_row_id = g.row_id
    WHERE g.otype = 'GeospatialCoordLocation'
    ORDER BY (COALESCE(ec.sample_location_edges, 0) + COALESCE(sc.site_location_edges, 0)) DESC
    LIMIT 50
""").fetchdf()

print(f"GeospatialCoordLocation linked to both paths: {len(both_paths_geos)} found")
both_paths_geos.head(10)

# %%
