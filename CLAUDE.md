# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Python Environment Management
- **Poetry** is the primary dependency manager (`pyproject.toml` manages dependencies)
- Install dependencies: `poetry install`
- Install with examples dependencies: `poetry install --with examples`
- Activate virtual environment: `poetry shell`
- Run Python scripts: `poetry run python <script.py>`

### Testing
- Run Python tests: `poetry run pytest tests/`
- Run single test: `poetry run pytest tests/test_isbclient.py::test_field_names`
- Test files are in `tests/` directory

### Playwright Testing (Web Scraping)
- Playwright tests located in `playwright/tests/`
- Run Playwright tests: `cd playwright && npx playwright test`
- View test reports: `cd playwright && npx playwright show-report`
- Configuration: `playwright/playwright.config.js`

### Docker Development
- Build and run Jupyter environment: `./run_docker.sh [port]`
- Default port is 8890, custom port can be specified as first argument
- Dockerfile creates a Jupyter environment with all dependencies installed

## Current Status & Issues ⚠️

**IMPORTANT**: As of September 2025, the iSamples central API at `https://central.isample.xyz/isamples_central/` is offline. This affects all three client classes below. The repository is transitioning to **offline-first geoparquet workflows** - see examples in `examples/basic/geoparquet.ipynb` and `examples/basic/isample-archive.ipynb` for working patterns.

## Architecture Overview

### Core Python Client (`src/isamples_client/`)
The main Python package provides three client classes for interacting with the iSamples API:

1. **`IsbClient`** (`isbclient.py:232-339`): Basic HTTP client using httpx
   - Direct API interaction with `/thing/select` endpoint
   - Methods: `field_names()`, `record_count()`, `facets()`, `pivot()`

2. **`IsbClient2`** (`isbclient.py:341-586`): Enhanced Solr client using pysolr
   - Extends IsbClient with more sophisticated search capabilities
   - Supports complex filter queries (`_fq_from_kwargs()`)
   - Default search parameters in `default_search_params()`
   - Faceting and pivot table functionality

3. **`ISamplesBulkHandler`** (`isbclient.py:588-683`): Bulk data operations
   - Handles large dataset exports via authentication
   - Methods: `create_download()`, `get_status()`, `download_file()`
   - Loads bulk data into pandas DataFrames

### Key Configuration Constants
- `ISB_SERVER`: Default iSamples API endpoint
- `FL_DEFAULT`: Default field list for search results
- `FACET_FIELDS_DEFAULT`: Default faceting fields
- `MAJOR_FIELDS`: UI field mappings
- `ISAMPLES_SOURCES`: Available data sources (SESAR, OPENCONTEXT, GEOME, SMITHSONIAN)

### Examples Structure
- **`examples/basic/`**: Basic API usage examples and Jupyter notebooks
- **`examples/spatial/`**: Geospatial data analysis with geoparquet, DuckDB
- **`examples/opencontext/`**: OpenContext-specific examples
- **`javascript/`**: JavaScript/Node.js integration examples

### Jupyter Notebook Integration
Heavy emphasis on Jupyter notebook examples for data exploration:
- Interactive data analysis with pandas, xarray
- Geospatial analysis using geopandas, folium, cartopy
- **Lonboard WebGL visualization**: High-performance point cloud rendering
- **DuckDB integration**: Efficient remote parquet processing via HTTP range requests
- **API-independent workflows**: Examples that work without central API access

### Dependencies Architecture
- **Core dependencies**: httpx, requests, pandas, xarray, pysolr
- **Spatial analysis**: geopandas, duckdb, polars, ibis-framework, shapely
- **Visualization**: matplotlib, folium, cartopy, ipyleaflet, lonboard
- **Jupyter ecosystem**: ipywidgets, ipydatagrid, sidecar

## Development Patterns

### Search Parameter Building
The codebase uses a sophisticated parameter building system:
- `_fq_from_kwargs()` builds Solr filter queries from keyword arguments
- Uses `multidict.MultiDict` for handling multiple values for same parameter
- Supports date range queries, source filtering, and complex boolean logic

### Error Handling and Logging
- Uses Python `logging` module (configured at INFO level)
- Request URLs are logged for debugging
- HTTP status codes checked with appropriate error raising

### Monkey Patching for Large Queries
- `monkey_patch_select()` modifies pysolr to handle large queries via POST
- `SWITCH_TO_POST` threshold (10000 bytes) determines GET vs POST usage
- Critical for handling complex search queries that exceed URL limits

## Known Issues & Troubleshooting

### API Connectivity Issues
- **Central API offline**: If you see connection errors to `https://central.isample.xyz/isamples_central/`, the API is currently offline
- **Workaround**: Use the geoparquet examples in `examples/basic/geoparquet.ipynb` and `examples/basic/isample-archive.ipynb` which work without API access
- **Alternative data sources**: The examples demonstrate accessing iSamples data via Zenodo archives and remote parquet files

### Lonboard Visualization Issues

**⚠️ CRITICAL: Lonboard 0.12+ API Breaking Change**

Lonboard 0.12+ changed how map initialization works. The old `zoom` and `center` parameters cause `TypeError`.

**OLD (BROKEN)**:
```python
viz(result, map_kwargs={"zoom": 1, "center": {"lat": 0, "lon": 0}})
```

**NEW (CORRECT for 0.12+)**:
```python
viz(result, map_kwargs={"view_state": {"zoom": 1, "latitude": 0, "longitude": 0}})
```

**Key changes**:
- `zoom` and `center` must be nested inside `view_state`
- `center: {lat, lon}` becomes flat `latitude` and `longitude` keys
- Dynamic updates: `m.set_view_state(longitude=..., latitude=..., zoom=...)`
- Animation: `m.fly_to(...)`

**Other considerations**:
- **Memory usage**: Always use `LIMIT` clauses when visualizing parquet data (e.g., `LIMIT 100000`)
- **Performance**: For 6M+ row datasets, querying without LIMIT can cause 5+ minute hangs
- **CRS warnings**: "No CRS exists on data" warnings are expected and can be ignored if lon/lat are WGS84
- **Deprecated**: The `con` parameter to `viz()` is deprecated in newer versions

### Environment Setup
- **Node.js conflicts**: Multiple `package.json` files exist; use `poetry install --with examples` for Python dependencies
- **Jupyter extensions**: Some notebooks require ipywidgets and sidecar extensions for full functionality