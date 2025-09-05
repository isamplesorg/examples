# Project Status - iSamples Python

**Last Updated**: 2025-09-05  
**Branch**: `exploratory`  
**Status**: Heavy WIP transitioning from API-dependent to geoparquet-focused workflows

## Current State Overview

This repository is in active development, pivoting from iSamples API integration to **offline-first geoparquet analysis** due to the central API being offline. The codebase contains excellent foundations for geological data visualization but has several loose ends that need resolution.

## 🚨 Critical Issues

### API Dependency Problems
- **Offline API**: `ISB_SERVER = "https://central.isample.xyz/isamples_central/"` is currently unreachable
- **No fallback mechanisms**: All three client classes (`IsbClient`, `IsbClient2`, `ISamplesBulkHandler`) will fail
- **Authentication workflows broken**: Bulk handler requires tokens from offline service

### Code Issues
- **lonboard parameter error** in `examples/basic/record_counts.ipynb:69`: Incorrect `zoom`/`center` parameters for Map constructor
- **Hardcoded paths**: Several notebooks contain user-specific paths that need generalization

## 🚧 Work In Progress Areas

### 1. Development Environment Inconsistencies
- **Mixed package managers**: Poetry (main) + npm scattered in multiple locations
  - Root: `package.json`, `package-lock.json`
  - `examples/basic/`: Node.js setup
  - `playwright/`: Separate npm environment
- **Node modules duplication**: `node_modules/` in multiple directories

### 2. JavaScript Integration Experiments
- **Incomplete experiments**: 
  - `examples/basic/hello_encode.js` - partial implementation
  - `examples/spatial/cesium_points.ipynb` - 3D visualization experiments
  - `javascript/stream.ipynb` - streaming data experiments
- **Playwright infrastructure**: Web scraping setup but unclear integration purpose

### 3. Incomplete Example Files
```
examples/basic/
├── bone.xlsx + ~$bone.xlsx (Excel temp file - should be cleaned)
├── subset.py (basic operations, undocumented)
├── zenodo_metadata.json (archival metadata, good for offline workflows)
└── hello_encode.js (incomplete JavaScript experiment)
```

### 4. Testing Infrastructure Gaps
- **Minimal test coverage**: Only basic structure in `tests/`
- **No integration tests**: For the core client classes
- **Playwright tests**: Present but targeting demo todo app, not iSamples functionality

## ✅ Working Well (Build On These)

### Excellent Visualization Patterns
- **`examples/basic/geoparquet.ipynb`** contains sophisticated lonboard code:
  - Zoom-layered rendering with `create_zoom_layers()`
  - Interactive color mapping by geological source
  - Efficient WebGL point cloud visualization
  - Well-documented functions for reuse

### Successful API-Free Workflows  
- **`examples/basic/isample-archive.ipynb`** demonstrates:
  - Remote parquet access via HTTP range requests
  - DuckDB efficient spatial queries
  - Zenodo archive integration
  - No API dependencies

### Robust Technology Stack
- **Core dependencies** properly managed in `pyproject.toml`
- **Spatial analysis tools**: GeoPandas, DuckDB, Shapely all working
- **Jupyter integration**: ipywidgets, sidecar, etc. functional

## 🎯 Recommended Next Steps

### Priority 1: API Issues
1. **Add offline detection** to client classes with graceful fallbacks
2. **Document workarounds** for API-dependent examples  
3. **Create mock data** for testing without API access

### Priority 2: Environment Consolidation
1. **Standardize Node.js usage**: Single package.json or eliminate if unnecessary
2. **Clean up temp files**: Remove `~$bone.xlsx` and similar artifacts
3. **Generalize paths**: Remove user-specific hardcoded paths from notebooks

### Priority 3: Documentation & Testing
1. **Complete examples/README.md**: Document each notebook's purpose and data requirements
2. **Add integration tests**: For successful offline workflows
3. **Create troubleshooting guide**: Common issues and solutions

### Priority 4: Code Quality
1. **Fix lonboard parameter errors** in record_counts notebook
2. **Extract reusable functions** from geoparquet.ipynb visualization code
3. **Add error handling** throughout the codebase

## 🔄 Strategic Direction

**From**: API-dependent geological data analysis  
**To**: Offline-first geoparquet workflows with modern spatial tools

**Key Success Metrics**:
- All examples run without API dependencies
- Clear documentation for new users
- Reusable visualization patterns
- Robust error handling

## Files Needing Immediate Attention

1. `examples/basic/record_counts.ipynb` - Fix lonboard Map parameters
2. `src/isamples_client/isbclient.py` - Add offline detection
3. `examples/basic/` - Clean up temporary/experimental files  
4. Root directory - Consolidate Node.js dependencies
5. `tests/` - Add meaningful test coverage

---

*This status document should be updated as issues are resolved and new ones discovered.*