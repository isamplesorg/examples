# Sections for Quarto Document Integration

## For `parquet_cesium.qmd`

### Section 1: After Introduction (Insert after line 9)

```markdown
## Understanding the Property Graph Structure

The OpenContext iSamples parquet file implements a sophisticated property graph model that combines the flexibility of graph databases with the analytical performance of columnar storage. Unlike traditional relational databases or pure graph databases, this approach stores both entities (nodes) and relationships (edges) in a single table structure.

### Why a Property Graph?

Archaeological and specimen data inherently forms a network:
- **Samples** are collected at **sites** during **events**
- **Sites** have **geographic locations**
- **Samples** have **material types** from controlled vocabularies
- **People** (agents) have various **roles** in the collection process

This interconnected nature makes a graph model ideal for representing the complex relationships while maintaining query performance.
```

### Section 2: Replace/Enhance "Table Structure Analysis" Section

```markdown
## Data Model Deep Dive

### Entity Types in the Dataset

The parquet file contains 7 distinct object types (`otype`), each serving a specific purpose in the archaeological data model:

```{ojs}
//| code-fold: true
entityTypeDescriptions = {
    return [
        {otype: "_edge_", purpose: "Relationships between entities", icon: "🔗"},
        {otype: "MaterialSampleRecord", purpose: "Physical samples/specimens", icon: "🪨"},
        {otype: "SamplingEvent", purpose: "When/how samples were collected", icon: "📅"},
        {otype: "GeospatialCoordLocation", purpose: "Geographic coordinates", icon: "📍"},
        {otype: "SamplingSite", purpose: "Archaeological sites/dig locations", icon: "🏛️"},
        {otype: "IdentifiedConcept", purpose: "Controlled vocabulary terms", icon: "📚"},
        {otype: "Agent", purpose: "People and organizations", icon: "👤"}
    ];
}

viewof entityTypeTable = Inputs.table(entityTypeDescriptions, {
    header: {
        otype: "Entity Type",
        purpose: "Purpose",
        icon: "Icon"
    }
})
```

### How Entities Connect: The Edge Model

Edges use a triple structure inspired by RDF:
- **Subject (s)**: The source entity's `row_id`
- **Predicate (p)**: The relationship type
- **Object (o)**: Array of target entity `row_id`s

This allows representing both simple (1:1) and complex (1:many) relationships efficiently.

```{ojs}
//| code-fold: true
// Visualize common relationship patterns
relationshipPatterns = {
    const query = `
        SELECT
            p as relationship,
            COUNT(*) as usage_count,
            COUNT(DISTINCT s) as unique_subjects
        FROM nodes
        WHERE otype = '_edge_'
        GROUP BY p
        ORDER BY usage_count DESC
        LIMIT 15
    `;
    const data = await loadData(query, [], "loading_relationships");
    return data;
}
```

<div id="loading_relationships">Loading relationship patterns...</div>

```{ojs}
//| code-fold: true
viewof relationshipTable = Inputs.table(relationshipPatterns, {
    header: {
        relationship: "Relationship Type",
        usage_count: "Total Uses",
        unique_subjects: "Unique Subjects"
    },
    format: {
        usage_count: d => d.toLocaleString(),
        unique_subjects: d => d.toLocaleString()
    }
})
```
```

### Section 3: Add Query Examples Section (Before Map)

```markdown
## Working with the Graph: Query Patterns

### Finding Samples with Locations

The most common need is connecting samples to their geographic coordinates. This requires traversing the graph through edges:

```{ojs}
//| code-fold: true
// Example: Get samples with direct location assignments
sampleLocationExample = {
    const query = `
        WITH sample_locations AS (
            SELECT
                s.pid as sample_id,
                s.label as sample_label,
                g.latitude,
                g.longitude,
                e.p as location_relationship
            FROM nodes s
            JOIN nodes e ON s.row_id = e.s
            JOIN nodes g ON e.o[1] = g.row_id
            WHERE s.otype = 'MaterialSampleRecord'
              AND e.otype = '_edge_'
              AND e.p = 'sample_location'
              AND g.otype = 'GeospatialCoordLocation'
            LIMIT 5
        )
        SELECT * FROM sample_locations
    `;
    const data = await loadData(query, [], "loading_sample_loc_example");
    return data;
}
```

<div id="loading_sample_loc_example">Loading example...</div>

```{ojs}
viewof sampleLocationTable = Inputs.table(sampleLocationExample, {
    layout: "auto"
})
```

### Multi-Hop Traversal: Sample → Event → Site → Location

Many samples don't have direct coordinates but are linked through their collection event and site:

```{ojs}
//| code-fold: true
// Trace the full chain from sample to site location
siteChainExample = {
    const query = `
        SELECT
            samp.pid as sample_id,
            event.pid as event_id,
            site.label as site_name,
            loc.latitude,
            loc.longitude
        FROM nodes samp
        JOIN nodes e1 ON samp.row_id = e1.s AND e1.p = 'produced_by'
        JOIN nodes event ON e1.o[1] = event.row_id
        JOIN nodes e2 ON event.row_id = e2.s AND e2.p = 'sampling_site'
        JOIN nodes site ON e2.o[1] = site.row_id
        JOIN nodes e3 ON site.row_id = e3.s AND e3.p = 'site_location'
        JOIN nodes loc ON e3.o[1] = loc.row_id
        WHERE samp.otype = 'MaterialSampleRecord'
          AND event.otype = 'SamplingEvent'
          AND site.otype = 'SamplingSite'
          AND loc.otype = 'GeospatialCoordLocation'
        LIMIT 5
    `;
    const data = await loadData(query, [], "loading_chain_example");
    return data;
}
```

<div id="loading_chain_example">Loading traversal example...</div>

```{ojs}
viewof siteChainTable = Inputs.table(siteChainExample, {
    layout: "auto",
    width: {
        sample_id: 150,
        event_id: 150,
        site_name: 200
    }
})
```
```

### Section 4: Performance Considerations (Add before closing)

```markdown
## Performance & Optimization Strategies

### Query Performance Guidelines

When working with this 11.6M row dataset:

1. **Filter Early**: Always apply `otype` filters first
   ```sql
   -- Good: Reduces to ~1M rows immediately
   WHERE otype = 'MaterialSampleRecord'

   -- Avoid: Scans all 11M rows
   WHERE label LIKE '%pottery%'
   ```

2. **Use Views for Complex Patterns**: Pre-compute common joins
   ```sql
   CREATE VIEW samples_with_coords AS
   SELECT ... -- complex join query
   ```

3. **Leverage DuckDB's Columnar Format**: Aggregate before detailed analysis
   ```sql
   -- Aggregate first, then filter
   WITH site_counts AS (
     SELECT site_id, COUNT(*) as cnt
     FROM ...
     GROUP BY site_id
   )
   SELECT * FROM site_counts WHERE cnt > 100
   ```

### Data Loading Strategies

For web applications:

```{ojs}
//| code-fold: true
// Progressive loading pattern for large datasets
progressiveLoadExample = {
    // Start with aggregated overview
    const overview = await db.query(`
        SELECT
            ROUND(latitude/10)*10 as lat_bucket,
            ROUND(longitude/10)*10 as lon_bucket,
            COUNT(*) as point_count
        FROM nodes
        WHERE otype = 'GeospatialCoordLocation'
        GROUP BY lat_bucket, lon_bucket
    `);

    // Load details on demand based on zoom/viewport
    // This reduces initial load from 200K to ~1K points

    return {
        strategy: "Progressive Loading",
        initial_points: overview.length,
        full_dataset: 198433,
        reduction_factor: Math.round(198433 / overview.length)
    };
}
```

```{ojs}
viewof loadStrategyDisplay = {
    const stats = await progressiveLoadExample;
    return html`<div style="padding: 1rem; background: #f0f9ff; border-radius: 8px;">
        <h4 style="margin-top: 0;">Loading Strategy Impact</h4>
        <p>Initial load: <strong>${stats.initial_points.toLocaleString()}</strong> aggregated points</p>
        <p>Full dataset: <strong>${stats.full_dataset.toLocaleString()}</strong> individual locations</p>
        <p>Reduction factor: <strong>${stats.reduction_factor}x</strong> faster initial load</p>
    </div>`;
}
```

### Handling Sensitive Location Data

Archaeological sites often require location protection:

```{ojs}
//| code-fold: true
obfuscationStats = {
    const query = `
        SELECT
            obfuscated,
            COUNT(*) as location_count,
            AVG(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) * 100 as pct_with_coords
        FROM nodes
        WHERE otype = 'GeospatialCoordLocation'
        GROUP BY obfuscated
    `;
    const data = await loadData(query, [], "loading_obfusc_stats");
    return data;
}
```

<div id="loading_obfusc_stats">Analyzing location sensitivity...</div>

```{ojs}
viewof obfuscationTable = Inputs.table(obfuscationStats, {
    header: {
        obfuscated: "Location Protection",
        location_count: "Count",
        pct_with_coords: "% With Coordinates"
    },
    format: {
        obfuscated: d => d ? "🔒 Protected" : "📍 Precise",
        location_count: d => d.toLocaleString(),
        pct_with_coords: d => d.toFixed(1) + "%"
    }
})
```

::: {.callout-important}
## Data Usage Note
When visualizing archaeological data, always respect location sensitivity flags. Obfuscated coordinates are intentionally imprecise to protect archaeological sites from looting.
:::
```

### Section 5: Advanced Analysis Examples

```markdown
## Advanced Analysis Patterns

### Material Type Distribution by Site

Understanding what types of materials are found at different archaeological sites:

```{ojs}
//| code-fold: true
materialBySite = {
    const query = `
        WITH site_materials AS (
            SELECT
                site.label as site_name,
                mat.name as material_type,
                COUNT(DISTINCT samp.row_id) as sample_count
            FROM nodes samp
            -- Sample to Event
            JOIN nodes e1 ON samp.row_id = e1.s AND e1.p = 'produced_by'
            JOIN nodes event ON e1.o[1] = event.row_id
            -- Event to Site
            JOIN nodes e2 ON event.row_id = e2.s AND e2.p = 'sampling_site'
            JOIN nodes site ON e2.o[1] = site.row_id
            -- Sample to Material Type
            JOIN nodes e3 ON samp.row_id = e3.s AND e3.p = 'has_material_category'
            JOIN nodes mat ON e3.o[1] = mat.row_id
            WHERE samp.otype = 'MaterialSampleRecord'
              AND site.otype = 'SamplingSite'
              AND mat.otype = 'IdentifiedConcept'
        )
        SELECT
            site_name,
            material_type,
            sample_count,
            SUM(sample_count) OVER (PARTITION BY site_name) as site_total
        FROM site_materials
        WHERE site_name IS NOT NULL
        ORDER BY site_total DESC, sample_count DESC
        LIMIT 50
    `;
    const data = await loadData(query, [], "loading_material_analysis");
    return data;
}
```

<div id="loading_material_analysis">Analyzing materials by site...</div>

```{ojs}
//| code-fold: true
// Group materials by site for visualization
materialPivot = {
    const sites = [...new Set(materialBySite.map(d => d.site_name))].slice(0, 10);
    const materials = [...new Set(materialBySite.map(d => d.material_type))];

    return {
        sites: sites,
        materials: materials,
        data: materialBySite.filter(d => sites.includes(d.site_name))
    };
}

Plot.plot({
    marginLeft: 150,
    marginBottom: 100,
    color: {
        scheme: "spectral"
    },
    marks: [
        Plot.cell(materialPivot.data, {
            x: "material_type",
            y: "site_name",
            fill: "sample_count",
            tip: true
        })
    ]
})
```

### Temporal Analysis

When samples were collected (where data is available):

```{ojs}
//| code-fold: true
temporalData = {
    const query = `
        SELECT
            result_time,
            COUNT(*) as sample_count
        FROM nodes
        WHERE otype = 'MaterialSampleRecord'
          AND result_time IS NOT NULL
          AND result_time != ''
        GROUP BY result_time
        ORDER BY result_time
    `;
    const data = await loadData(query, [], "loading_temporal");
    return data;
}
```

<div id="loading_temporal">Loading temporal data...</div>

This dataset primarily contains archaeological specimens where precise collection dates are often unknown or represent historical periods rather than specific dates.
```