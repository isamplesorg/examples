# OpenContext iSamples Parquet Documentation

## Data Model Overview

The OpenContext iSamples parquet file (`oc_isamples_pqg.parquet`) implements a **property graph model** stored in a single table. This approach balances the flexibility of a full graph database with the analytical performance of columnar storage.

### File Statistics
- **Size**: ~691 MB
- **Total Rows**: 11,637,144
- **Structure**: Combined nodes (entities) and edges (relationships) in one table
- **URL**: `https://storage.googleapis.com/opencontext-parquet/oc_isamples_pqg.parquet`

### Property Graph Concept

In this model:
- **Nodes** represent entities (samples, locations, events, etc.)
- **Edges** represent relationships between entities
- Both are stored in the same table, distinguished by the `otype` field
- Edges use `otype='_edge_'` while nodes use specific entity types

## Entity Types (Object Types)

| Object Type | Count | Description |
|------------|-------|-------------|
| `_edge_` | 9,201,451 | Relationships between entities |
| `SamplingEvent` | 1,096,352 | When/how a sample was collected |
| `MaterialSampleRecord` | 1,096,352 | The actual sample records |
| `GeospatialCoordLocation` | 198,433 | Geographic coordinates (lat/lon) |
| `IdentifiedConcept` | 25,778 | Controlled vocabulary terms |
| `SamplingSite` | 18,213 | Archaeological sites/locations |
| `Agent` | 565 | People/organizations (collectors, curators) |

## Core Schema Fields

### Identity & Metadata
- `row_id`: Integer primary key (sequential)
- `pid`: Persistent identifier (globally unique string)
- `otype`: Object type (see entity types above)
- `tcreated`, `tmodified`: Timestamps (integer)
- `altids`: Alternative identifiers array

### Graph Structure (for edges)
- `s`: Subject/source (row_id of source node)
- `p`: Predicate/relationship type (string)
- `o`: Object/target array (row_ids of target nodes)
- `n`: Named graph context

### Geospatial Fields
- `latitude`, `longitude`: Decimal degrees
- `geometry`: WKB geometry blob
- `place_name`: Location names array
- `obfuscated`: Boolean flag for sensitive locations
- `elevation`: String representation

### Sample-Specific Fields
- `sample_identifier`: Primary sample ID
- `has_feature_of_interest`: What the sample represents
- `sampling_purpose`: Why collected
- `material_type`: Physical composition
- `curation_location`: Where stored

### Metadata & Rights
- `dc_rights`: Rights statement
- `access_constraints`: Access restrictions array
- `thumbnail_url`: Visual representation
- `label`, `description`: Human-readable text

## Entity Relationships

The graph uses edges to connect entities. Common relationship patterns:

### Sample → Location
```
MaterialSampleRecord --[sample_location]--> GeospatialCoordLocation
```

### Sample → Event → Site
```
MaterialSampleRecord --[produced_by]--> SamplingEvent
SamplingEvent --[sampling_site]--> SamplingSite
SamplingSite --[site_location]--> GeospatialCoordLocation
```

### Sample → Agent
```
MaterialSampleRecord --[responsibility]--> Agent (collector)
MaterialSampleRecord --[registrant]--> Agent (data contributor)
```

### Sample → Concepts
```
MaterialSampleRecord --[has_material_category]--> IdentifiedConcept
MaterialSampleRecord --[has_sample_object_type]--> IdentifiedConcept
MaterialSampleRecord --[has_context_category]--> IdentifiedConcept
```

## Common Query Patterns

### 1. Get All Samples with Direct Locations

```sql
-- Find samples with direct geographic coordinates
WITH sample_locations AS (
    SELECT
        s.pid as sample_id,
        s.label as sample_label,
        g.latitude,
        g.longitude,
        g.place_name
    FROM oc_pqg s
    JOIN oc_pqg e ON s.row_id = e.s  -- Join through edge
    JOIN oc_pqg g ON e.o[1] = g.row_id  -- Join to location
    WHERE s.otype = 'MaterialSampleRecord'
      AND e.otype = '_edge_'
      AND e.p = 'sample_location'
      AND g.otype = 'GeospatialCoordLocation'
)
SELECT * FROM sample_locations
WHERE latitude IS NOT NULL;
```

### 2. Trace Sample to Site Location

```sql
-- Follow the chain: Sample -> Event -> Site -> Location
WITH sample_site_chain AS (
    SELECT
        samp.pid as sample_id,
        samp.label as sample_label,
        event.pid as event_id,
        site.pid as site_id,
        site.label as site_name,
        loc.latitude,
        loc.longitude
    FROM oc_pqg samp
    -- Sample to Event
    JOIN oc_pqg e1 ON samp.row_id = e1.s AND e1.p = 'produced_by'
    JOIN oc_pqg event ON e1.o[1] = event.row_id
    -- Event to Site
    JOIN oc_pqg e2 ON event.row_id = e2.s AND e2.p = 'sampling_site'
    JOIN oc_pqg site ON e2.o[1] = site.row_id
    -- Site to Location
    JOIN oc_pqg e3 ON site.row_id = e3.s AND e3.p = 'site_location'
    JOIN oc_pqg loc ON e3.o[1] = loc.row_id
    WHERE samp.otype = 'MaterialSampleRecord'
      AND event.otype = 'SamplingEvent'
      AND site.otype = 'SamplingSite'
      AND loc.otype = 'GeospatialCoordLocation'
)
SELECT * FROM sample_site_chain;
```

### 3. Find Sample Material Types

```sql
-- Get samples with their material classifications
SELECT
    s.pid as sample_id,
    s.label as sample_label,
    c.pid as concept_id,
    c.label as material_type,
    c.name as material_category
FROM oc_pqg s
JOIN oc_pqg e ON s.row_id = e.s
JOIN oc_pqg c ON e.o[1] = c.row_id
WHERE s.otype = 'MaterialSampleRecord'
  AND e.otype = '_edge_'
  AND e.p = 'has_material_category'
  AND c.otype = 'IdentifiedConcept';
```

### 4. Aggregate by Site

```sql
-- Count samples per site with coordinates
SELECT
    site.label as site_name,
    COUNT(DISTINCT samp.row_id) as sample_count,
    AVG(loc.latitude) as avg_lat,
    AVG(loc.longitude) as avg_lon
FROM oc_pqg samp
JOIN oc_pqg e1 ON samp.row_id = e1.s AND e1.p = 'produced_by'
JOIN oc_pqg event ON e1.o[1] = event.row_id
JOIN oc_pqg e2 ON event.row_id = e2.s AND e2.p = 'sampling_site'
JOIN oc_pqg site ON e2.o[1] = site.row_id
JOIN oc_pqg e3 ON site.row_id = e3.s AND e3.p = 'site_location'
JOIN oc_pqg loc ON e3.o[1] = loc.row_id
WHERE samp.otype = 'MaterialSampleRecord'
GROUP BY site.label
ORDER BY sample_count DESC;
```

### 5. Extract Subgraph for Visualization

```sql
-- Get all entities and relationships for a specific site
WITH target_site AS (
    SELECT row_id FROM oc_pqg
    WHERE otype = 'SamplingSite'
    AND label LIKE '%Pompeii%'
    LIMIT 1
),
related_edges AS (
    SELECT * FROM oc_pqg
    WHERE otype = '_edge_'
    AND (s IN (SELECT row_id FROM target_site)
         OR array_contains(o, (SELECT row_id FROM target_site)))
),
related_nodes AS (
    SELECT DISTINCT node.* FROM oc_pqg node
    WHERE node.row_id IN (
        SELECT s FROM related_edges
        UNION
        SELECT unnest(o) FROM related_edges
    )
)
SELECT * FROM related_nodes
UNION ALL
SELECT * FROM related_edges;
```

## Performance Optimization Tips

### 1. Use CTEs for Complex Joins
Common Table Expressions make multi-hop queries more readable and sometimes faster:

```sql
WITH locations AS (
    SELECT * FROM oc_pqg WHERE otype = 'GeospatialCoordLocation'
),
samples AS (
    SELECT * FROM oc_pqg WHERE otype = 'MaterialSampleRecord'
)
-- Use the CTEs in your main query
```

### 2. Filter Early
Apply `otype` filters first to reduce the dataset:

```sql
-- Good: Filter by otype first
WHERE otype = 'MaterialSampleRecord' AND label LIKE '%pottery%'

-- Less efficient: Complex condition first
WHERE label LIKE '%pottery%' AND otype = 'MaterialSampleRecord'
```

### 3. Create Views for Common Patterns

```sql
-- Create a view for samples with locations
CREATE VIEW samples_with_locations AS
SELECT
    s.row_id,
    s.pid as sample_id,
    s.label,
    g.latitude,
    g.longitude,
    g.place_name
FROM oc_pqg s
JOIN oc_pqg e ON s.row_id = e.s
JOIN oc_pqg g ON e.o[1] = g.row_id
WHERE s.otype = 'MaterialSampleRecord'
  AND e.otype = '_edge_'
  AND e.p = 'sample_location'
  AND g.otype = 'GeospatialCoordLocation';
```

### 4. Handle Array Fields Efficiently

```sql
-- Unnest array fields when needed
SELECT
    pid,
    unnest(place_name) as individual_place_name
FROM oc_pqg
WHERE otype = 'GeospatialCoordLocation'
  AND place_name IS NOT NULL;
```

### 5. Use Appropriate Aggregations

```sql
-- For large-scale spatial analysis, aggregate first
SELECT
    ROUND(latitude, 1) as lat_bin,
    ROUND(longitude, 1) as lon_bin,
    COUNT(*) as location_count
FROM oc_pqg
WHERE otype = 'GeospatialCoordLocation'
GROUP BY lat_bin, lon_bin;
```

## Data Quality Considerations

### Check for Orphaned Nodes
```sql
-- Find nodes that aren't referenced by any edges
SELECT COUNT(*) as orphan_count
FROM oc_pqg n
WHERE n.otype NOT IN ('_edge_')
  AND NOT EXISTS (
    SELECT 1 FROM oc_pqg e
    WHERE e.otype = '_edge_'
    AND (e.s = n.row_id OR array_contains(e.o, n.row_id))
  );
```

### Validate Coordinate Ranges
```sql
-- Check for invalid coordinates
SELECT COUNT(*) as invalid_coords
FROM oc_pqg
WHERE otype = 'GeospatialCoordLocation'
  AND (latitude < -90 OR latitude > 90
       OR longitude < -180 OR longitude > 180);
```

### Find Duplicate PIDs
```sql
-- PIDs should be unique
SELECT pid, COUNT(*) as count
FROM oc_pqg
GROUP BY pid
HAVING COUNT(*) > 1;
```

## Working with Obfuscated Locations

Some locations are intentionally obscured for site protection:

```sql
-- Separate obfuscated from precise locations
SELECT
    obfuscated,
    COUNT(*) as count,
    COUNT(DISTINCT pid) as unique_locations
FROM oc_pqg
WHERE otype = 'GeospatialCoordLocation'
GROUP BY obfuscated;
```

## Integration Notes

### For Jupyter Notebooks
- Use DuckDB's Python API for direct parquet access
- Leverage pandas DataFrames for analysis
- Consider ipywidgets for interactive filtering

### For Web Visualization
- Pre-aggregate data to reduce payload
- Use the Cesium integration for 3D globe rendering
- Consider progressive loading for large datasets

### For Graph Analysis
- Export subgraphs to NetworkX or similar tools
- Use the edge table structure for relationship analysis
- Consider graph metrics (degree, centrality) for important nodes