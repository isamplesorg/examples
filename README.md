# isamples-python

Python examples for exploring **6.7 million material samples** from scientific collections worldwide using high-performance geoparquet analysis and visualization.

## Quick Start

```bash
# Install dependencies
poetry install --with examples

# Activate environment
poetry shell

# Launch Jupyter for examples
jupyter lab examples/basic/
```

## Overview

This repository provides Jupyter notebooks for analyzing material sample data from the iSamples project. The iSamples metadata model is **domain-agnostic**, supporting samples from geology, biology, archaeology, environmental science, and other fields.

**Data Sources:**
- **SESAR**: 4.6M geological samples
- **OpenContext**: 1M archaeological samples
- **GEOME**: 605K genomic samples
- **Smithsonian**: 322K museum specimens

All data accessed via **geoparquet files** on Cloudflare R2 - no API required.

## Key Examples

| Notebook | Description |
|----------|-------------|
| **`isamples_explorer.ipynb`** ⭐ | Interactive explorer with faceted search, map/table views |
| **`geoparquet.ipynb`** ⭐ | Advanced lonboard visualization with zoom-layered rendering |
| **`pqg_demo.ipynb`** | Property graph queries with DuckDB |
| **`schema_comparison.ipynb`** | Narrow vs wide format comparison |
| **`isample-archive.ipynb`** | Remote parquet analysis via DuckDB |

See [examples/README.md](examples/README.md) for detailed descriptions.

## Technology Stack

- **Queries**: DuckDB on remote parquet via HTTP range requests
- **Visualization**: Lonboard (WebGL), Matplotlib, Folium
- **Data Processing**: Pandas, GeoPandas, Polars
- **Jupyter**: IPyWidgets, IPyDatagrid, Sidecar

## Data Access

All examples use parquet files served from Cloudflare R2:

```python
import duckdb

# Wide format (~280 MB, 20M rows)
WIDE_URL = "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"

# Query directly - DuckDB fetches only needed data
con = duckdb.connect()
df = con.sql(f"SELECT * FROM '{WIDE_URL}' WHERE n = 'OPENCONTEXT' LIMIT 1000").df()
```

## Development

```bash
# Install with examples dependencies
poetry install --with examples

# Run tests
poetry run pytest tests/

# Docker Jupyter environment
./run_docker.sh [port]  # default port 8890
```

## Archived Code

The original API client (`src/isamples_client/`) targeted `https://central.isample.xyz/isamples_central/` which is **no longer operational**. This code has been moved to `archive/defunct-api-client/` for historical reference.

Planning documents from the API-to-parquet transition are in `archive/planning/`.

## Related iSamples Repositories

| Repo | Purpose | Start Here |
|------|---------|------------|
| [isamplesorg-metadata](https://github.com/isamplesorg/metadata) | Schema definition (8 types, 14 predicates) | `src/schemas/isamples_core.yaml` |
| [isamplesorg.github.io](https://isamplesorg.github.io/) | Browser tutorials (DuckDB-WASM + Cesium) | `tutorials/isamples_explorer.qmd` |
| [vocabularies](https://github.com/isamplesorg/vocabularies) | SKOS vocabulary terms | Material types, context categories |

## Related Technologies

- [Lonboard](https://github.com/developmentseed/lonboard) - Fast geospatial visualization
- [DuckDB](https://duckdb.org/) - High-performance analytical database
- [iSamples Project](https://www.isamples.org/) - Internet of Samples initiative
