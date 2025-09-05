# iSamples Examples

This directory contains Jupyter notebooks and scripts demonstrating different approaches to working with geological sample data from the iSamples project.

## 🌟 Key Notebooks (Start Here)

### `basic/geoparquet.ipynb` ⭐ **PRIMARY VISUALIZATION EXAMPLE**
**Status**: ✅ Working (API-independent)  
**Focus**: Advanced lonboard WebGL visualization

**What it does**:
- Loads geological sample data from geoparquet files
- Creates sophisticated WebGL point cloud visualizations using Lonboard
- Implements zoom-layered rendering for performance with large datasets
- Provides interactive controls for filtering by geological source collections
- Demonstrates advanced color mapping and styling techniques

**Key patterns to reuse**:
```python
# Zoom-layered visualization
layers = create_zoom_layers(gdf, zoom_levels, color_map)

# Interactive filtering  
layer = update_layer_colors(gdf_data, selected_collections)

# Efficient color mapping
colors = create_color_map(data, color_map, selected_collections)
```

### `basic/isample-archive.ipynb` 
**Status**: ✅ Working (API-independent)  
**Focus**: Remote parquet analysis with DuckDB

**What it does**:
- Accesses iSamples data via Zenodo archives using HTTP range requests
- Demonstrates efficient spatial queries using DuckDB on remote parquet files  
- Shows how to work with geological data without API dependencies
- Examples of spatial filtering, aggregation, and analysis

**Key pattern**:
```python
# Remote parquet access
conn = duckdb.connect()
result = conn.sql("SELECT * FROM 'https://zenodo.org/.../data.parquet'")
```

### `basic/record_counts.ipynb`
**Status**: ⚠️ Has issues (lonboard parameter errors)  
**Focus**: Quick visualization patterns

**What it does**:
- Demonstrates rapid prototyping with `lonboard.viz()`
- Shows basic DuckDB operations on local data
- Quick visualization of record counts and distributions

**Known issues**:
- Line 69: Incorrect `zoom`/`center` parameters for `Map()` constructor
- Needs parameter fixes for lonboard compatibility

## 📁 Directory Structure

```
examples/
├── basic/                    # Core examples and tutorials
│   ├── geoparquet.ipynb     ⭐ Main visualization notebook
│   ├── isample-archive.ipynb  ✅ Remote parquet analysis  
│   ├── record_counts.ipynb    ⚠️ Quick patterns (has issues)
│   ├── pgp.ipynb             🧪 Additional lonboard experiments
│   ├── subset.py             📝 Basic Python subset operations
│   ├── bone.xlsx             📊 Sample Excel data
│   └── zenodo_metadata.json  📋 Archive metadata
│
├── spatial/                  # Advanced spatial analysis
│   ├── cesium_points.ipynb   🌐 3D visualization experiments
│   ├── cities.geoparquet     🗺️ Sample spatial data
│   └── bay_area_cities.parquet
│
├── opencontext/             # OpenContext-specific examples
└── javascript/              # Node.js integration experiments
    └── stream.ipynb         🔄 Streaming data patterns
```

## 🚀 Getting Started

### Prerequisites
```bash
# Install dependencies
poetry install --with examples

# Activate environment
poetry shell

# Launch Jupyter
jupyter lab
```

### Recommended Learning Path

1. **Start with `basic/geoparquet.ipynb`** - Learn advanced lonboard visualization
2. **Try `basic/isample-archive.ipynb`** - Understand remote data access  
3. **Explore `spatial/cesium_points.ipynb`** - See 3D visualization options
4. **Check `basic/pgp.ipynb`** - Additional lonboard patterns

## 📊 Data Sources

### Working Data Sources (API-independent)
- **Zenodo archives**: Remote parquet files accessible via HTTP
- **Local geoparquet files**: Sample data in `spatial/` directory  
- **Excel samples**: `bone.xlsx` for testing data import

### API-dependent Sources ⚠️
- **iSamples Central API**: Currently offline (`https://central.isample.xyz/isamples_central/`)
- **Bulk export endpoints**: Require authentication from offline API

## 🛠️ Common Patterns

### Lonboard Visualization
```python
from lonboard import Map, ScatterplotLayer
from lonboard.colormap import apply_continuous_cmap

# Basic pattern
layer = ScatterplotLayer.from_geopandas(
    gdf,
    get_fill_color=colors,
    get_radius=300,
    radius_units='meters',
    pickable=True
)
map_widget = Map(layers=[layer])
```

### DuckDB Remote Access
```python
import duckdb

# Connect and query remote parquet
conn = duckdb.connect()
result = conn.sql("SELECT * FROM 'remote_file.parquet' WHERE condition")
gdf = result.to_df()
```

### Error Handling for API Issues
```python
try:
    # API-dependent code
    client = IsbClient()
    data = client.search()
except requests.exceptions.ConnectionError:
    # Fallback to local/remote parquet
    print("API unavailable, using local data")
    data = pd.read_parquet('backup_data.parquet')
```

## 🐛 Troubleshooting

### Common Issues

1. **"No CRS exists on data" warning**
   - Usually harmless for visualization
   - Add explicit CRS if needed: `gdf.set_crs('EPSG:4326')`

2. **Lonboard Map parameter errors**
   - Don't use `zoom` and `center` directly in `Map()` constructor
   - Use layer-specific parameters instead

3. **Memory issues with large datasets**
   - Use the zoom-layered approach from `geoparquet.ipynb`
   - Sample data before visualization: `gdf.sample(n=10000)`

4. **API connection errors**
   - Expected behavior - API is currently offline
   - Use geoparquet examples for working patterns

### Performance Tips

- **Large datasets**: Use DuckDB for filtering before loading into memory
- **Interactive maps**: Implement zoom-based level-of-detail rendering
- **Memory usage**: Sample data for initial exploration, full dataset for final analysis

## 🔗 Related Documentation

- [Main README](../README.md) - Repository overview
- [STATUS.md](../STATUS.md) - Current issues and WIP areas  
- [CLAUDE.md](../CLAUDE.md) - Development guidance
- [Lonboard Documentation](https://github.com/developmentseed/lonboard)
- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial)

---

*This README is updated as new examples are added and issues are resolved.*