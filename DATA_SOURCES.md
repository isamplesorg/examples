# Shared Data Sources - iSamples Ecosystem

**Maintained by**: Both `isamples-python` and `isamplesorg.github.io` repositories  
**Last Updated**: 2025-09-05

## Primary Data Sources

### Zenodo iSamples Archive ⭐ **PRIMARY**
- **URL**: `https://z.rslv.xyz/10.5281/zenodo.15278210/isamples_export_2025_04_21_16_23_46_geo.parquet`
- **Size**: ~300 MB, 6+ million records
- **Format**: Geoparquet with spatial indexing
- **Sources**: SESAR, OpenContext, GEOME, Smithsonian (all federated sources)
- **Update Frequency**: Periodic (check Zenodo for latest versions)
- **Access Method**: HTTP range requests for efficient querying
- **CORS Status**: ⚠️ Check current accessibility for browser use

**Data Quality Notes**:
- Comprehensive geological sample metadata
- Spatial coordinates available for most records
- Some records may have missing or incomplete fields
- Quality varies by source system

### OpenContext Collections
- **Base URL Pattern**: Various URLs for specific archaeological collections
- **Format**: Parquet files with domain-specific schemas
- **Access**: HTTP range requests supported
- **Usage**: Domain-specific analysis, educational examples

### Local Sample Data (Both Repos)

#### In `isamples-python/examples/spatial/`:
- `cities.geoparquet` - Sample cities data for testing
- `bay_area_cities.parquet` - Regional subset for performance testing
- Purpose: Development and testing without external dependencies

#### In `isamplesorg.github.io` tutorials:
- Embedded fallback datasets for CORS-restricted environments
- Demo datasets demonstrating same analytical techniques
- Smaller scale data for educational purposes

## Data Access Patterns

### Python Environment (`isamples-python`)
```python
import duckdb

# Connect to DuckDB and query remote parquet
conn = duckdb.connect()
result = conn.sql("""
    SELECT source, COUNT(*) as sample_count
    FROM 'https://z.rslv.xyz/10.5281/zenodo.15278210/isamples_export_2025_04_21_16_23_46_geo.parquet'
    GROUP BY source
""")
df = result.to_df()
```

### Browser Environment (`isamplesorg.github.io`)
```javascript
// DuckDB-WASM with automatic CORS fallback
const conn = await duckdb.connect();

// Primary data source with fallback
const dataUrl = "https://z.rslv.xyz/10.5281/zenodo.15278210/isamples_export_2025_04_21_16_23_46_geo.parquet";

try {
    const result = await conn.query(`
        SELECT source, COUNT(*) as sample_count
        FROM '${dataUrl}'
        GROUP BY source
    `);
} catch (e) {
    // Fallback to demo dataset
    console.log("CORS blocked, using demo data");
    // ... fallback logic
}
```

## Performance Characteristics

### HTTP Range Request Benefits
- **Metadata queries**: <1KB transfer for table statistics
- **Sampling**: ~1-10KB for representative samples  
- **Filtered queries**: Only transfers matching data rows
- **Aggregations**: Minimal data transfer for GROUP BY operations

### Memory Usage
- **Browser**: Analyze 300MB datasets in <100MB memory
- **Python**: Full dataset can be loaded for complex operations
- **Streaming**: Both environments support streaming for larger-than-memory analysis

## Data Update Coordination

### Version Management
1. **Check Zenodo** regularly for updated iSamples exports
2. **Test compatibility** in both Python and browser environments
3. **Update URLs** in both repositories simultaneously
4. **Verify data quality** with standard validation queries

### Validation Queries
```sql
-- Basic quality checks (run in both environments)
SELECT 
    source,
    COUNT(*) as total_records,
    COUNT(latitude) as records_with_coords,
    MIN(collection_date) as earliest_date,
    MAX(collection_date) as latest_date
FROM parquet_file
GROUP BY source;
```

### Update Process
1. **Identify new data source** on Zenodo or other archives
2. **Test in Python environment** first (full DuckDB capabilities)
3. **Test in browser environment** (check CORS, performance)
4. **Update both repositories** with new URLs and documentation
5. **Verify examples still work** in both environments

## Known Issues & Workarounds

### CORS Restrictions
- **Problem**: Some data sources block browser access
- **Detection**: Try HEAD request first in browser tutorials  
- **Workaround**: Automatic fallback to demo datasets
- **Solution**: Host CORS-enabled mirrors when possible

### Data Quality Issues
- **Missing coordinates**: ~5-10% of records may lack spatial data
- **Encoding issues**: Some text fields may have inconsistent encoding
- **Date formats**: Multiple date formats across source systems
- **Null values**: Handle missing data gracefully in all queries

### Performance Considerations
- **Large queries**: Use LIMIT in initial development/testing
- **Memory limits**: Browser environment more constrained than Python
- **Network timeouts**: Implement retry logic for large HTTP range requests

## Cross-Repository Testing

### Shared Test Queries
Both repositories should validate these standard queries work:

```sql
-- Test 1: Basic connectivity and record count
SELECT COUNT(*) FROM parquet_file;

-- Test 2: Source distribution  
SELECT source, COUNT(*) FROM parquet_file GROUP BY source;

-- Test 3: Spatial data availability
SELECT 
    COUNT(*) as total,
    COUNT(latitude) as with_coords,
    ROUND(100.0 * COUNT(latitude) / COUNT(*), 2) as coord_percentage
FROM parquet_file;

-- Test 4: Date range analysis
SELECT 
    source,
    MIN(collection_date) as earliest,
    MAX(collection_date) as latest
FROM parquet_file 
WHERE collection_date IS NOT NULL
GROUP BY source;
```

### Expected Results (as of 2025-04-21 export)
- Total records: ~6+ million
- Sources: SESAR, OpenContext, GEOME, Smithsonian
- Spatial coverage: Global with concentrations in North America, Europe
- Date range: Historical to present (varies by source)

## Contact & Coordination

### Data Issues
- Report data quality issues in both repository issue trackers
- Tag issues with `data-quality` label for visibility
- Include specific queries and expected vs actual results

### New Data Sources
- Propose new data sources in `isamples-python` issues
- Test compatibility in both environments before adoption
- Document access patterns and any special considerations

---

*This document is maintained collaboratively between both repositories to ensure consistency and coordination.*