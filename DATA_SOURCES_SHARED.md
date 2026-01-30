# iSamples Data Sources (Shared Reference)

This document defines the canonical data sources used across all iSamples repositories.

## Canonical Parquet URLs (Cloudflare R2)

All tutorials and examples should reference these URLs:

```python
# Wide format (RECOMMENDED for most use cases)
# - 280 MB, 20M rows
# - All entity types, no edges
# - Flattened lat/lon columns for easy querying
WIDE_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"

# Narrow format (for advanced property graph queries)
# - 850 MB, 106M rows
# - Includes edge rows for relationship traversal
NARROW_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202512_narrow.parquet"
```

## Data Summary

| Source | Samples | Description |
|--------|---------|-------------|
| **SESAR** | 4.6M | Geological samples (rock cores, sediments, minerals) |
| **OpenContext** | 1.0M | Archaeological samples (pottery, bones, artifacts) |
| **GEOME** | 605K | Genomic/biological samples (tissue, DNA) |
| **Smithsonian** | 322K | Museum specimens (natural history collections) |
| **Total** | 6.7M | Material samples across scientific domains |

## Schema Formats

### Wide Format (Recommended)
- **Use case**: Visualization, filtering, basic queries
- **Structure**: One row per entity (no edge rows)
- **Columns**: 47 including `latitude`, `longitude`, `label`, `n` (source)
- **Relationships**: Stored as `p__*` array columns (e.g., `p__has_material_category`)

### Narrow Format (Advanced)
- **Use case**: Property graph traversal, relationship analysis
- **Structure**: Separate rows for nodes and edges
- **Columns**: Normalized `s`, `p`, `o`, `n` fields
- **Relationships**: Explicit edge rows with predicate in `p` column

## Example Queries

### DuckDB (Python/CLI)
```python
import duckdb

WIDE_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"
con = duckdb.connect()

# Count by source
con.sql(f"""
    SELECT n as source, COUNT(*) as count
    FROM '{WIDE_URL}'
    WHERE otype = 'MaterialSampleRecord'
    GROUP BY n
""").show()
```

### DuckDB-WASM (Browser)
```javascript
const WIDE_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet";

const result = await db.query(`
    SELECT n as source, COUNT(*) as count
    FROM '${WIDE_URL}'
    WHERE otype = 'MaterialSampleRecord'
    GROUP BY n
`);
```

## Related Repositories

| Repo | Purpose | Data Usage |
|------|---------|------------|
| [isamplesorg-metadata](https://github.com/isamplesorg/metadata) | Schema definition | Defines the 8 entity types + 14 predicates |
| [isamples-python](https://github.com/isamplesorg/examples) | Jupyter examples | Queries parquet with DuckDB + Lonboard |
| [isamplesorg.github.io](https://isamplesorg.github.io/) | Browser tutorials | Queries parquet with DuckDB-WASM + Cesium |
| [vocabularies](https://github.com/isamplesorg/vocabularies) | SKOS terms | Material types, context categories |

## Version History

| Date | Format | URL Suffix | Notes |
|------|--------|------------|-------|
| 2026-01 | Wide | `isamples_202601_wide.parquet` | Fixed null array bug (#8) |
| 2025-12 | Narrow | `isamples_202512_narrow.parquet` | Added MaterialSampleCuration |
| 2025-04 | Export | (Zenodo archive) | Original export format |

---

*This file is canonical across iSamples repositories. Update here, then sync to other repos.*
