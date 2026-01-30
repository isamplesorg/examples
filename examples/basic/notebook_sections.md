# Sections for Jupyter Notebook Integration

## For `oc_parquet_analysis.ipynb`

### Section 1: After Initial Setup (Add as Markdown Cell)

```markdown
## Understanding the Data Structure

This parquet file uses a **property graph model** where both entities (nodes) and relationships (edges) are stored in a single table. The `otype` field determines whether a row is:
- An entity (e.g., `MaterialSampleRecord`, `GeospatialCoordLocation`)
- A relationship (`_edge_`) connecting entities

Key insight: To get meaningful data, you'll often need to JOIN through edges to connect samples to their locations, events, or other properties.
```

### Section 2: After Schema Display (Add as Code + Markdown Cells)

```python
# Examine the distribution of entity types in detail
entity_stats = conn.execute("""
    SELECT
        otype,
        COUNT(*) as count,
        COUNT(DISTINCT pid) as unique_pids,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM oc_pqg
    GROUP BY otype
    ORDER BY count DESC
""").fetchdf()

print("Entity Type Distribution:")
print(entity_stats)
```

```markdown
### Graph Structure Fields

The fields `s`, `p`, `o`, `n` are used for edges:
- **s** (subject): row_id of the source entity
- **p** (predicate): the type of relationship
- **o** (object): array of target row_ids
- **n** (name): graph context (usually null)

Example: A sample (s) has_material_category (p) pointing to a concept (o).
```

### Section 3: Practical Query Examples (Add as Code Cells)

```python
# Query 1: Find all samples with geographic coordinates
samples_with_coords = conn.execute("""
    SELECT
        s.pid as sample_id,
        s.label as sample_label,
        s.description,
        g.latitude,
        g.longitude,
        g.place_name
    FROM oc_pqg s
    JOIN oc_pqg e ON s.row_id = e.s
    JOIN oc_pqg g ON e.o[1] = g.row_id
    WHERE s.otype = 'MaterialSampleRecord'
      AND e.otype = '_edge_'
      AND e.p = 'sample_location'
      AND g.otype = 'GeospatialCoordLocation'
      AND g.latitude IS NOT NULL
    LIMIT 100
""").fetchdf()

print(f"Found {len(samples_with_coords)} samples with coordinates")
samples_with_coords.head()
```

```python
# Query 2: Trace samples through events to sites
sample_site_hierarchy = conn.execute("""
    WITH sample_to_site AS (
        SELECT
            samp.pid as sample_id,
            samp.label as sample_label,
            event.pid as event_id,
            site.pid as site_id,
            site.label as site_name
        FROM oc_pqg samp
        JOIN oc_pqg e1 ON samp.row_id = e1.s AND e1.p = 'produced_by'
        JOIN oc_pqg event ON e1.o[1] = event.row_id AND event.otype = 'SamplingEvent'
        JOIN oc_pqg e2 ON event.row_id = e2.s AND e2.p = 'sampling_site'
        JOIN oc_pqg site ON e2.o[1] = site.row_id AND site.otype = 'SamplingSite'
        WHERE samp.otype = 'MaterialSampleRecord'
    )
    SELECT
        site_name,
        COUNT(*) as sample_count
    FROM sample_to_site
    GROUP BY site_name
    ORDER BY sample_count DESC
    LIMIT 20
""").fetchdf()

print("Top archaeological sites by sample count:")
print(sample_site_hierarchy)
```

```python
# Query 3: Explore material types and categories
material_analysis = conn.execute("""
    SELECT
        c.label as material_type,
        c.name as category_name,
        COUNT(DISTINCT s.row_id) as sample_count
    FROM oc_pqg s
    JOIN oc_pqg e ON s.row_id = e.s
    JOIN oc_pqg c ON e.o[1] = c.row_id
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
```

### Section 4: Performance Tips (Add as Markdown Cell)

```markdown
## Query Performance Tips

1. **Always filter by `otype` first** - This dramatically reduces the search space
2. **Use CTEs (WITH clauses)** for complex multi-hop queries
3. **Limit results during exploration** - Add `LIMIT 1000` while testing queries
4. **Create views for common patterns** - Reuse complex joins

### Memory Management
For the full 11M row dataset:
- Simple counts and filters: Fast (<1 second)
- Single-hop joins: Moderate (1-5 seconds)
- Multi-hop joins: Can be slow (5-30 seconds)
- Full table scans: Avoid without filters
```

### Section 5: Visualization Preparation (Add as Code Cell)

```python
# Prepare data for map visualization
def get_sample_locations_for_viz(limit=10000):
    """Extract sample locations optimized for visualization"""

    return conn.execute(f"""
        WITH located_samples AS (
            SELECT
                s.pid as sample_id,
                s.label as label,
                g.latitude,
                g.longitude,
                g.obfuscated,
                e.p as location_type
            FROM oc_pqg s
            JOIN oc_pqg e ON s.row_id = e.s
            JOIN oc_pqg g ON e.o[1] = g.row_id
            WHERE s.otype = 'MaterialSampleRecord'
              AND e.otype = '_edge_'
              AND e.p IN ('sample_location', 'sampling_site')
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
        FROM located_samples
        WHERE NOT obfuscated  -- Exclude obfuscated locations for public viz
        LIMIT {limit}
    """).fetchdf()

# Get visualization-ready data
viz_data = get_sample_locations_for_viz(5000)
print(f"Prepared {len(viz_data)} samples for visualization")
print(f"Coordinate bounds: Lat [{viz_data.latitude.min():.2f}, {viz_data.latitude.max():.2f}], "
      f"Lon [{viz_data.longitude.min():.2f}, {viz_data.longitude.max():.2f}]")
```

### Section 6: Data Export Options (Add as Code Cell)

```python
# Export subsets for external analysis
def export_site_subgraph(site_name_pattern, output_prefix):
    """Export all data related to a specific site"""

    # Find the site
    site_info = conn.execute("""
        SELECT row_id, pid, label
        FROM oc_pqg
        WHERE otype = 'SamplingSite'
        AND label LIKE ?
        LIMIT 1
    """, [f'%{site_name_pattern}%']).fetchdf()

    if site_info.empty:
        print(f"No site found matching '{site_name_pattern}'")
        return

    site_row_id = site_info.iloc[0]['row_id']
    print(f"Exporting data for: {site_info.iloc[0]['label']}")

    # Get all related entities
    related_data = conn.execute("""
        WITH RECURSIVE related AS (
            -- Start with the site
            SELECT row_id FROM oc_pqg WHERE row_id = ?

            UNION

            -- Add entities connected by edges
            SELECT DISTINCT unnest(e.o) as row_id
            FROM oc_pqg e
            JOIN related r ON e.s = r.row_id
            WHERE e.otype = '_edge_'

            UNION

            SELECT DISTINCT e.s as row_id
            FROM oc_pqg e
            JOIN related r ON array_contains(e.o, r.row_id)
            WHERE e.otype = '_edge_'
        )
        SELECT p.*
        FROM oc_pqg p
        JOIN related r ON p.row_id = r.row_id
    """, [site_row_id]).fetchdf()

    # Save to parquet
    output_file = f"{output_prefix}_{site_info.iloc[0]['pid']}.parquet"
    related_data.to_parquet(output_file)
    print(f"Exported {len(related_data)} rows to {output_file}")

    return related_data

# Example usage (commented out)
# pompeii_data = export_site_subgraph("Pompeii", "pompeii_subgraph")
```