# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Development Commands

### Python Environment Management
- **Poetry** is the primary dependency manager (`pyproject.toml` manages dependencies)
- Install dependencies: `poetry install`
- Install with examples dependencies: `poetry install --with examples`
- Activate virtual environment: `poetry shell`
- Run Python scripts: `poetry run python <script.py>`

### Testing
- Run Python tests: `poetry run pytest tests/`
- Test files are in `tests/` directory

### Docker Development
- Build and run Jupyter environment: `./run_docker.sh [port]`
- Default port is 8890

## Repository Overview

This repository provides Jupyter notebooks for exploring **6.7M material samples** using geoparquet files. All data accessed via Cloudflare R2 - no API required.

### Core Examples (`examples/basic/`)

| Notebook | Purpose |
|----------|---------|
| `isamples_explorer.ipynb` ⭐ | Interactive explorer with faceted search, map/table views |
| `geoparquet.ipynb` ⭐ | Advanced lonboard visualization with zoom-layered rendering |
| `pqg_demo.ipynb` | Property graph queries with DuckDB |
| `schema_comparison.ipynb` | Narrow vs wide format comparison |
| `isample-archive.ipynb` | Remote parquet analysis via DuckDB |

### Archived Code

| Location | Contents |
|----------|----------|
| `archive/defunct-api-client/` | Original API client (API offline since 2025) |
| `archive/planning/` | Planning docs from API-to-parquet transition |
| `examples/basic/archive/` | Superseded notebook experiments |

## Data Sources

All data served from Cloudflare R2:

```python
# Wide format (~280 MB, 20M rows) - preferred
WIDE_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"

# Narrow format (~850 MB, 106M rows) - includes edge rows
NARROW_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202512_narrow.parquet"
```

**Source counts:**
- SESAR: 4.6M geological samples
- OpenContext: 1M archaeological samples
- GEOME: 605K genomic samples
- Smithsonian: 322K museum specimens

## Notebook Editing & Version Control

**For Claude Code and Git Workflows**:

1. **jupytext pairing** (recommended for active development):
   - Pair `.ipynb` with `.py` companions: `~/bin/nb_pair.sh notebook.ipynb`
   - Edit `.py` files to avoid token limits (no outputs in source)
   - See: `JUPYTEXT_WORKFLOW.md` for full guide

2. **nb_source_diff.py** (for quick diffs):
   - Diff notebooks without output noise: `nb-diff notebook.ipynb HEAD`

**Quick Reference**: See `QUICKREF_NOTEBOOKS.md` for command cheatsheet

## Technology Stack

- **Queries**: DuckDB on remote parquet via HTTP range requests
- **Visualization**: Lonboard (WebGL), Matplotlib, Folium, Cartopy
- **Data Processing**: Pandas, GeoPandas, Polars, Ibis
- **Jupyter**: IPyWidgets, IPyDatagrid, Sidecar

## Known Issues

### Lonboard 0.12+ API Breaking Change

Lonboard 0.12+ changed how map initialization works.

**OLD (BROKEN)**:
```python
viz(result, map_kwargs={"zoom": 1, "center": {"lat": 0, "lon": 0}})
```

**NEW (CORRECT for 0.12+)**:
```python
viz(result, map_kwargs={"view_state": {"zoom": 1, "latitude": 0, "longitude": 0}})
```

### Performance Tips

- **Always use `LIMIT`** when visualizing parquet data (e.g., `LIMIT 100000`)
- Querying 6M+ rows without LIMIT can cause 5+ minute hangs
- CRS warnings ("No CRS exists on data") can be ignored if lon/lat are WGS84
