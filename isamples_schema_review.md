# iSamples Parquet Schema Comparison - Review Request

## Context: Project Wrap-Up

The iSamples project (NSF-funded) is wrapping up. We need to:
1. Archive data to Zenodo for long-term preservation
2. Build a lightweight frontend UI for scientists to explore samples
3. Demonstrate value to NSF program officers

The central API server is being decommissioned - all future access must be via static parquet files served from cloud storage (Cloudflare R2, GCS).

## Three Schema Formats

We have three parquet representations of the same iSamples data:

### 1. Export Format (Zenodo archive)
- **Philosophy**: Sample-centric, fully denormalized
- **Rows**: 6.7M (one per sample)
- **Columns**: 19 (including nested STRUCTs)
- **File size**: ~300MB
- **Key features**:
  - Nested STRUCTs for relationships (produced_by, curation, has_material_category)
  - Pre-extracted coordinates at top level (sample_location_latitude/longitude)
  - No JOINs needed for sample queries

### 2. PQG Narrow Format (Property Graph)
- **Philosophy**: Graph-normalized with explicit edge rows
- **Rows**: 92M (11.6M entities + 80M+ edge rows)
- **Columns**: 40
- **File size**: ~725MB
- **Key features**:
  - Separate `_edge_` rows with s/p/o (subject/predicate/object)
  - Entity types: MaterialSampleRecord, SamplingEvent, SamplingSite, GeospatialCoordLocation, IdentifiedConcept, Agent
  - Full graph traversal flexibility
  - Most normalized representation

### 3. PQG Wide Format
- **Philosophy**: Entity-centric with relationship arrays
- **Rows**: 19.5M (2.5M entities, no edge rows)
- **Columns**: 47
- **File size**: ~290MB
- **Key features**:
  - Relationships stored as `p__*` arrays of row_ids
  - No edge rows - relationships embedded in entities
  - Middle ground between Export and Narrow

## Benchmark Results (from schema_comparison.ipynb)

| Query Pattern | Export | Wide | Narrow |
|--------------|--------|------|--------|
| Map (all coords) | 41ms (6M pts) | 7ms (200K pts) | 16ms (200K pts) |
| Facets (material counts) | ~500ms | ~2s | ~3s |
| Entity listing (all agents) | Slow (scan all samples) | Fast (direct filter) | Fast (direct filter) |
| Sample detail | 1 row, everything | Need JOINs | Need JOINs + edge traversal |

## Key Insight: Export Has More Points!

Export format returns 6M coordinate points vs ~200K for PQG formats because:
- Export has pre-extracted coordinates for ALL samples across ALL sources (SESAR, GEOME, Smithsonian, OpenContext)
- PQG files in this comparison are OpenContext-only

## Optimization Axes We're Balancing

1. **Query Performance**: Export wins for sample-centric queries (no JOINs)
2. **Storage Efficiency**: Wide (~290MB) < Export (~300MB) < Narrow (~725MB)
3. **Entity Independence**: Narrow/Wide win (can query agents/sites directly)
4. **Graph Flexibility**: Narrow wins (arbitrary traversals)
5. **Archival Fidelity**: Narrow preserves most information
6. **UI Simplicity**: Export wins (no understanding of graph model needed)

## Eric's 3-Part Plan for Final Deliverables

**Part 1**: PostgreSQL dump → PQG export → Zenodo archive
- Preserve data for posterity

**Part 2**: Frontend-optimized parquet
- H3 geohash for initial map aggregation
- Pre-computed facet counts
- Click-to-table interaction

**Part 3**: Visual enhancements
- Collection logos (SESAR, OpenContext, GEOME, Smithsonian)
- NounProject icons for sample types

## Questions for Review

1. **Is Export format the right choice for the frontend UI?** Given that scientists primarily want to browse/filter samples, not traverse graphs?

2. **Should we maintain all three formats or consolidate?** What's the cost/benefit of each?

3. **For the H3 geohash optimization** (Part 2), should this be a separate file or derived view?

4. **The `list_contains()` problem**: Both Wide (p__* arrays) and Export (nested structs) require O(n) scans. Is there a better approach for "samples by agent" queries?

5. **Architecture recommendation**: Given the constraints (static files, browser-based DuckDB-WASM, no server), what's the optimal data architecture?

## Files for Reference

- Schema definitions: `pqg/pqg/schemas/{narrow,wide,export}.py`
- Comparison notebook: `isamples-python/examples/basic/schema_comparison.ipynb`
- Project status: `isamples-python/STATUS.md`
