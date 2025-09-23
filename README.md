# isamples-python

Python client library and examples for working with iSamples geological data, with a focus on high-performance geoparquet analysis and visualization.

## Quick Start

```bash
# Install dependencies
poetry install --with examples

# Activate environment  
poetry shell

# Launch Jupyter for examples
jupyter lab examples/
```

## Overview

This repository provides Python tools for analyzing geological sample data from the iSamples project. Originally designed to work with the iSamples API, it has evolved to focus on **offline-first, geoparquet-centric workflows** using modern spatial data tools.

### Key Capabilities

- **High-performance visualization** with [Lonboard](https://github.com/developmentseed/lonboard) WebGL mapping
- **Efficient spatial queries** using DuckDB on remote parquet files
- **Interactive Jupyter notebooks** for geological data exploration
- **API-independent workflows** accessing data via HTTP range requests

## Architecture

### Python Client Library (`src/isamples_client/`)

Three client classes for different use cases:

1. **`IsbClient`**: Basic HTTP client using httpx
2. **`IsbClient2`**: Enhanced Solr client with complex query support  
3. **`ISamplesBulkHandler`**: Bulk data operations with authentication

**Note**: API clients currently target `https://central.isample.xyz/isamples_central/` which may be offline. See [STATUS.md](STATUS.md) for current issues and workarounds.

### Key Examples

- **`examples/basic/geoparquet.ipynb`** ⭐ - Advanced lonboard visualization with zoom-layered rendering
- **`examples/basic/oc_parquet_analysis_enhanced.ipynb`** ⭐ - **NEW**: OpenContext property graph analysis with Ibis and DuckDB
- **`examples/basic/isample-archive.ipynb`** - Remote parquet analysis via DuckDB
- **`examples/basic/record_counts.ipynb`** - Quick visualization patterns
- **`examples/basic/oc_parquet_analysis.ipynb`** - Basic OpenContext parquet exploration

The enhanced OpenContext notebook demonstrates:
- **Property graph traversal** through complex multi-hop joins
- **Ibis vs raw SQL** comparison for readable query construction
- **Corrected relationship paths** for sample-to-location queries
- **Performance optimization** techniques for 11M+ row datasets

See [examples/README.md](examples/README.md) for detailed notebook descriptions.

## Technology Stack

- **Spatial Analysis**: GeoPandas, DuckDB, Shapely, **Ibis** (new)
- **Visualization**: Lonboard, Matplotlib, Folium, Cartopy
- **Data Processing**: Pandas, Polars, PyArrow
- **Jupyter Ecosystem**: IPyWidgets, IPyDatagrid, Sidecar

## Development

### Commands

```bash
# Install with examples dependencies
poetry install --with examples

# Run tests
poetry run pytest tests/

# Run Playwright tests (web scraping)
cd playwright && npx playwright test

# Docker Jupyter environment
./run_docker.sh [port]  # default port 8890
```

### Current Focus: Geoparquet Workflows

This repository is transitioning from API-dependent to **offline-first geoparquet analysis**:

- ✅ Remote parquet processing via DuckDB HTTP range requests
- ✅ High-performance WebGL visualization with Lonboard
- ✅ Interactive geological data exploration notebooks
- 🚧 API fallback mechanisms and error handling
- 🚧 Consolidated development environment

See [STATUS.md](STATUS.md) for detailed WIP status and loose ends.

## Ecosystem Integration

### Companion Repository: [isamplesorg.github.io](https://github.com/isamplesorg/isamplesorg.github.io)
**Public website with browser-based tutorials and documentation**

**Complementary roles**:
- 🔗 **This repo (`isamples-python`)**: Local development, advanced analysis, Python ecosystem
- 🌐 **Website repo**: Public tutorials, universal browser access, educational content  

**Shared technology**: Both use DuckDB + geoparquet for efficient data analysis
- Same data sources (Zenodo archives, HTTP range requests)
- Compatible visualization approaches (lonboard ↔ Observable Plot)
- Coordinated development patterns

See [CROSS_REPO_ALIGNMENT.md](CROSS_REPO_ALIGNMENT.md) for detailed integration strategy.

## Related Projects

- [iSamples](https://www.isamples.org/) - Internet of Samples project
- [Lonboard](https://github.com/developmentseed/lonboard) - Fast geospatial visualization
- [DuckDB](https://duckdb.org/) - High-performance analytical database
