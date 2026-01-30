# PQG Integration Plan for oc_parquet_analysis_enhanced.ipynb

**Date**: 2025-11-11
**Goal**: Integrate the `pqg` library to simplify graph operations while preserving domain-specific logic

---

## Executive Summary

The notebook currently has **100 cells** with **29 recursive CTEs** and complex SQL joins for property graph traversal. The `pqg` library can simplify many of these operations while maintaining performance for OpenContext-specific analysis.

**Strategy**: Hybrid approach
- ✅ Use PQG for: Node retrieval, edge traversal, entity queries
- ✅ Keep custom SQL for: Visualization aggregations, bulk operations, performance-critical queries
- ✅ Add comparison sections showing both approaches

---

## Current State Analysis

### 🔍 What the Notebook Does

**Core Functions (11 custom query functions)**:
1. `get_sample_locations_for_viz()` - Extract samples with coordinates for mapping
2. `get_sample_geo_context_via_sample_pid()` - Get all geographic context for a sample
3. `get_samples_for_geo_pid()` - Reverse: get samples at a geographic location
4. `export_site_subgraph()` - Export all data for a site pattern
5. **Eric's 4 authoritative queries** (already implemented in SQL):
   - `get_sample_data_via_sample_pid()`
   - `get_sample_data_agents_sample_pid()`
   - `get_sample_types_and_keywords_via_sample_pid()`
   - `get_samples_at_geo_cord_location_via_sample_event()`
6. `get_sampling_sites_by_name()` - Site search by name pattern
7. `ark_to_url()` - URL conversion utility

**Heavy SQL Patterns**:
- **29 cells** use CTEs/recursive queries for graph traversal
- **35 cells** use multi-table joins
- **20 cells** use aggregation (GROUP BY)
- **15 cells** filter by entity type (WHERE otype=...)

**Domain-Specific Logic**:
- Path 1 vs Path 2 geographic traversal patterns
- Three-category geo classification (both paths, Path 1 only, Path 2 only)
- Site-level vs event-level coordinate analysis
- Material type categorization
- Data quality checks (orphaned nodes, location quality)

---

## Integration Strategy

### Phase 1: Setup & Basic Queries (Easy Wins)

**Replace simple entity queries with PQG methods**

#### 1.1 Entity Type Counts
**Current** (Cell 10):
```python
# SQL
SELECT otype, COUNT(*) as count FROM pqg
WHERE otype != '_edge_'
GROUP BY otype
```

**With PQG**:
```python
from pqg import PQG

# Load parquet as PQG instance
pqg_instance = PQG(conn)
pqg_instance._table = 'pqg'
pqg_instance._isparquet = True

# Get entity type distribution
entity_counts = {}
for pid, otype in pqg_instance.getIds(maxrows=100000):
    entity_counts[otype] = entity_counts.get(otype, 0) + 1

# Or use SQL for performance on large datasets (keep current approach)
```

**Recommendation**: Keep SQL for aggregations (faster), use PQG for learning/examples

#### 1.2 Edge Predicate Exploration
**Current** (Cell 12):
```python
SELECT p, COUNT(*) FROM pqg WHERE otype='_edge_' GROUP BY p
```

**With PQG**:
```python
# Get all relationships
predicates = {}
for subject, predicate, obj in pqg_instance.getRelations():
    predicates[predicate] = predicates.get(predicate, 0) + 1
```

**Recommendation**: Keep SQL (much faster for 9M+ edges)

#### 1.3 Node Retrieval by PID
**Current**: Direct SQL SELECT
**With PQG**:
```python
# Get a single node with all properties
sample_node = pqg_instance.getNode("ark:/28722/k2wq0b20z", max_depth=0)
print(sample_node)

# With depth=1, automatically expands related nodes
sample_with_relations = pqg_instance.getNode("ark:/28722/k2wq0b20z", max_depth=1)
```

**Recommendation**: ✅ **USE PQG** - Simpler API, handles row_id conversion automatically

---

### Phase 2: Graph Traversal Functions (Medium Complexity)

**Replace custom recursive CTEs with PQG traversal methods**

#### 2.1 `get_sample_geo_context_via_sample_pid()`

**Current approach**: Multi-hop SQL join
```sql
-- Find event
SELECT e.o[1] as event_pid
FROM pqg e
WHERE e.s = (SELECT row_id FROM pqg WHERE pid = sample_pid)
  AND e.p = 'produced_by'

-- Find geo via event
SELECT g.pid FROM pqg g
JOIN pqg e ON g.row_id = e.o[1]
WHERE e.s = event_row_id AND e.p = 'sample_location'
```

**With PQG**:
```python
def get_sample_geo_context_via_sample_pid_pqg(pqg_instance, sample_pid):
    """Get geographic context using PQG graph traversal"""

    # Get sample node with edges expanded (depth=1 gets immediate neighbors)
    sample = pqg_instance.getNode(sample_pid, max_depth=1)

    # Navigate to event via produced_by edge
    event_pid = sample.get('produced_by')  # Auto-expanded by max_depth=1
    if not event_pid:
        return None

    # Get event with its edges
    event = pqg_instance.getNode(event_pid, max_depth=1)

    # Extract geographic context
    geo_context = {
        'sample_location': event.get('sample_location'),  # Path 1
        'sampling_site': event.get('sampling_site')       # Path 2 (site)
    }

    return geo_context
```

**Comparison**:
- PQG: More readable, handles row_id conversion, 3 API calls
- SQL: Faster for bulk, single query, but complex
- **Recommendation**: ✅ **Show both** - PQG for clarity, SQL for performance

#### 2.2 `get_samples_for_geo_pid()` - Reverse Traversal

**Current**: Complex SQL with UNION for Path 1 + Path 2
**With PQG**:
```python
def get_samples_for_geo_pid_pqg(pqg_instance, geo_pid, mode='either_or'):
    """Find samples connected to a geographic location (reverse traversal)"""

    # Path 1: geo <- sample_location <- event <- produced_by <- sample
    path1_samples = []
    for subj, pred, obj in pqg_instance.getRelations(obj=geo_pid, predicate='sample_location'):
        event_pid = subj  # Event that has this geo as sample_location
        event = pqg_instance.getNode(event_pid, max_depth=1)

        # Find samples produced by this event
        for s2, p2, o2 in pqg_instance.getRelations(obj=event_pid, predicate='produced_by'):
            sample_pid = s2
            path1_samples.append(sample_pid)

    # Path 2: geo <- site_location <- site <- sampling_site <- event <- produced_by <- sample
    # (Similar pattern, more hops)

    return path1_samples
```

**Comparison**:
- PQG: Clear step-by-step traversal
- SQL: Single query with joins, much faster
- **Recommendation**: ✅ **Show both** - PQG for learning, SQL for production

---

### Phase 3: Domain-Specific Optimizations (Keep Custom)

**These should remain as custom SQL - they're OpenContext-specific and performance-critical**

#### 3.1 Visualization Queries
**Keep as SQL**:
- `get_sample_locations_for_viz()` - Optimized for 10K+ samples, specific column selection
- Geographic classification queries (3-category analysis)
- Coordinate extraction for mapping

**Reason**: Need bulk aggregation, specific projections, performance-critical

#### 3.2 Eric's Authoritative Queries
**Keep as SQL**:
- All 4 of Eric's queries are already optimized and tested
- They use specific column selections not available in PQG API
- Performance-critical for web UI

**Reason**: Production-tested, web application integration

#### 3.3 Data Quality Analysis
**Keep as SQL**:
- Orphaned node detection
- Location quality checks
- Summary statistics

**Reason**: Require full table scans and aggregations

---

## Implementation Plan

### Step 1: Add PQG Setup Section (New Cell)

Insert after Cell 6 (data loading):

```python
# === PQG Integration Setup ===

from pqg import PQG

# Create PQG instance from loaded parquet
def create_pqg_instance(conn, table_name='pqg'):
    """Initialize PQG wrapper around parquet data"""
    pqg_instance = PQG(dbinstance=conn)
    pqg_instance._table = table_name
    pqg_instance._isparquet = True  # Read-only mode
    pqg_instance._node_pk = 'pid'   # Primary lookup field
    return pqg_instance

pqg_instance = create_pqg_instance(conn)

print("✅ PQG instance created")
print(f"Table: {pqg_instance._table}")
print(f"Read-only mode: {pqg_instance._isparquet}")
```

### Step 2: Add Comparison Sections (Incremental)

For each major query pattern, add a comparison cell:

```markdown
### Example: Node Retrieval - SQL vs PQG

**SQL Approach** (current):
```python
# [existing SQL query]
```

**PQG Approach** (alternative):
```python
# [PQG equivalent]
```

**Performance Comparison**:
- SQL: X seconds
- PQG: Y seconds
- **Use SQL for**: Bulk operations, aggregations
- **Use PQG for**: Single node traversal, learning, prototyping
```

### Step 3: Rewrite 3 Key Functions with PQG (Examples)

Choose 3 representative functions to show PQG alternative:

1. ✅ `get_sample_geo_context_via_sample_pid()` - Forward traversal
2. ✅ `get_samples_for_geo_pid()` - Reverse traversal
3. ✅ `export_site_subgraph()` - Subgraph extraction

**Implementation**:
- Create `_pqg` suffixed versions alongside originals
- Add timing comparisons
- Document when to use each

### Step 4: Add "PQG Learning Section" (New)

New section at end of notebook:

```markdown
## Using PQG for Interactive Exploration

This section demonstrates using the PQG library for interactive graph exploration.
Use these patterns for prototyping and learning. For production queries, use the
optimized SQL versions shown earlier.

### Basic Operations
- Node retrieval: `pqg_instance.getNode(pid)`
- Edge queries: `pqg_instance.getRelations(subject=..., predicate=...)`
- Entity search: `pqg_instance.getIds(otype="MaterialSampleRecord")`

### Graph Traversal
[Examples of multi-hop traversal]

### When to Use PQG vs SQL
[Decision matrix]
```

---

## Decision Matrix: PQG vs Custom SQL

| Use Case | PQG | Custom SQL | Rationale |
|----------|-----|------------|-----------|
| Single node lookup | ✅ | ⚠️ | PQG handles row_id conversion, cleaner API |
| Multi-hop traversal (1-3 hops) | ✅ | ⚠️ | PQG more readable, acceptable performance |
| Reverse graph traversal | ⚠️ | ✅ | SQL more efficient for finding "what points to X" |
| Bulk aggregations (10K+ rows) | ❌ | ✅ | SQL dramatically faster |
| Visualization queries | ❌ | ✅ | Need specific projections, performance-critical |
| Data quality analysis | ❌ | ✅ | Requires full table scans |
| Learning/prototyping | ✅ | ⚠️ | PQG clearer for understanding graph structure |
| Production web queries | ❌ | ✅ | Eric's queries already optimized and tested |

**Legend**:
- ✅ Recommended
- ⚠️ Works but not optimal
- ❌ Not recommended

---

## Expected Benefits

### Code Clarity
**Before**:
```sql
-- 30 lines of recursive CTE SQL
WITH RECURSIVE traverse AS (...)
SELECT ... FROM traverse JOIN ...
```

**After**:
```python
# 5 lines of PQG
sample = pqg_instance.getNode(sample_pid, max_depth=1)
event = pqg_instance.getNode(sample['produced_by'], max_depth=1)
geo = event['sample_location']
```

### Learning Value
- **New users** can understand graph structure via PQG API
- **SQL experts** can see equivalent SQL for optimization
- **Comparison sections** show tradeoffs

### Maintainability
- Less SQL to maintain for simple queries
- PQG handles schema changes (row_id conversion)
- Clear separation: PQG for exploration, SQL for production

---

## Risks & Mitigations

### Risk 1: Performance Regression
**Concern**: PQG might be slower for large queries
**Mitigation**:
- ✅ Keep all existing SQL queries as primary
- ✅ Add PQG as **alternative** in comparison sections
- ✅ Benchmark and document performance differences

### Risk 2: API Limitations
**Concern**: PQG might not support all OpenContext-specific patterns
**Mitigation**:
- ✅ Use hybrid approach - PQG for basics, SQL for advanced
- ✅ Document gaps in "When to Use PQG vs SQL" section
- ✅ Contribute improvements back to pqg library if needed

### Risk 3: Notebook Complexity
**Concern**: Adding PQG might make notebook harder to follow
**Mitigation**:
- ✅ Use collapsible sections for alternatives
- ✅ Clear headers: "SQL Approach" vs "PQG Approach"
- ✅ Summary tables showing when to use each

---

## Success Criteria

After integration, the notebook should:

1. ✅ **Preserve all existing functionality** - Every query still works
2. ✅ **Show PQG alternatives** for 5-10 common patterns
3. ✅ **Include performance comparisons** - Clear benchmarks
4. ✅ **Have clear guidance** - Decision matrix for when to use each
5. ✅ **Be more accessible** - New users can learn via PQG, then optimize with SQL
6. ✅ **Maintain performance** - Production queries unchanged

---

## Next Steps

### Immediate (30 minutes)
1. ✅ Add PQG setup cell (Step 1)
2. ✅ Test basic operations (`getNode()`, `getRelations()`)
3. ✅ Verify row_id conversion works correctly

### Short-term (2-3 hours)
4. ✅ Rewrite 1 function with PQG: `get_sample_geo_context_via_sample_pid()`
5. ✅ Add comparison section with timing
6. ✅ Document findings

### Medium-term (1-2 sessions)
7. ✅ Add 2 more PQG function examples
8. ✅ Create "PQG Learning Section" at end
9. ✅ Add decision matrix to README

### Long-term (optional)
10. ⏭️ Extract common patterns into helper module
11. ⏭️ Contribute enhancements back to pqg library
12. ⏭️ Create tutorial notebook: "Graph Queries with PQG"

---

## Questions to Resolve

1. **Performance baseline**: What's acceptable slowdown for PQG clarity benefits?
   - Suggestion: 2-3x slower OK for single-node queries, not for bulk

2. **API gaps**: Does PQG support reverse traversal efficiently?
   - Need to test `getRelations(obj=geo_pid)` performance

3. **Integration pattern**: Separate notebook or integrated sections?
   - **Recommendation**: Integrated comparison sections (more useful)

4. **Documentation location**: Where to put "When to use PQG" guide?
   - **Recommendation**: Both in notebook AND in isamples-python README

---

**Prepared by**: Claude Code (Sonnet 4.5)
**Date**: 2025-11-11
**Next Action**: Discuss this plan, then implement Step 1 (PQG setup)
