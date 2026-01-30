# Project Status - iSamples Python

**Last Updated**: 2026-01-29
**Status**: Geoparquet-focused workflows (API client archived)

## Current State

This repository provides Jupyter notebooks for exploring 6.7M material samples using **geoparquet files** served from Cloudflare R2. No API access required.

### Core Examples (Working)

| Notebook | Purpose | Status |
|----------|---------|--------|
| `isamples_explorer.ipynb` | Interactive explorer with faceted search | ✅ Active |
| `geoparquet.ipynb` | Lonboard visualization patterns | ✅ Active |
| `pqg_demo.ipynb` | Property graph queries | ✅ Active |
| `schema_comparison.ipynb` | Narrow vs wide format | ✅ Active |
| `isample-archive.ipynb` | Remote parquet analysis | ✅ Active |

### Archived (Historical Reference)

| Location | Contents |
|----------|----------|
| `archive/defunct-api-client/` | Original API client code (API offline since 2025) |
| `archive/planning/` | Planning docs from API-to-parquet transition |
| `examples/basic/archive/` | Superseded notebook experiments |

## Data Sources

All data served from Cloudflare R2:

- **Wide format**: `https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet` (~280 MB, 20M rows)
- **Narrow format**: `https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202512_narrow.parquet` (~850 MB, 106M rows)

## Known Issues

1. **Lonboard 0.12+ API change**: Use `view_state` instead of `zoom`/`center` parameters
2. **Large queries**: Always use `LIMIT` clauses when visualizing (100K+ points can hang)

## Technology Stack

- **Queries**: DuckDB on remote parquet via HTTP range requests
- **Visualization**: Lonboard (WebGL), Matplotlib, Folium
- **Data Processing**: Pandas, GeoPandas, Polars
- **Jupyter**: IPyWidgets, IPyDatagrid, Sidecar
