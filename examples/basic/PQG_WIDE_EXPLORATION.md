# PQG Wide Schema Exploration Summary

**Date**: 2025-12-01
**Context**: Eric Kansa created an experimental "wide" serialization of PQG data

## Key Insight

**The 14 ISamplesEdgeType values define the GRAMMAR (semantics) of iSamples relationships.**

Narrow vs Wide = different SERIALIZATIONS of the same grammar:

| Aspect | Narrow Schema | Wide Schema |
|--------|---------------|-------------|
| Storage | Edge rows (`otype='_edge_'`, `s`, `p`, `o`) | Predicate columns (`p__produced_by`, etc.) |
| Same 14 edge types? | ✅ Yes | ✅ Yes |
| Query pattern | 7 joins (3 edge + 4 entity tables) | 3 joins (entity tables only) |
| Optimized for | Write flexibility | Read performance |

## Performance Comparison (Local Files)

| Metric | Narrow | Wide | Improvement |
|--------|--------|------|-------------|
| File size | 690.9 MB | 275.3 MB | **60% smaller** |
| Row count | 11.6M | 2.5M | **79% fewer** (no edge rows) |
| Geolocation query | 142ms | 54ms | **2.6x faster** |

## Files

- **Narrow**: `oc_isamples_pqg.parquet` (690.9 MB)
- **Wide**: `oc_isamples_pqg_wide.parquet` (275.3 MB)
- **Wide source**: `https://storage.googleapis.com/opencontext-parquet/oc_isamples_pqg_wide.parquet`

## Edge Type → Column Mapping

10 of 14 edge types exist in OpenContext data:

| Edge Type | p__* Column | Has Data? |
|-----------|-------------|-----------|
| MSR_PRODUCED_BY | `p__produced_by` | ✅ 1.1M |
| MSR_REGISTRANT | `p__registrant` | ✅ 422K |
| MSR_KEYWORDS | `p__keywords` | ✅ 1.1M |
| MSR_HAS_CONTEXT_CATEGORY | `p__has_context_category` | ✅ 1.1M |
| MSR_HAS_MATERIAL_CATEGORY | `p__has_material_category` | ✅ 1.1M |
| MSR_HAS_SAMPLE_OBJECT_TYPE | `p__has_sample_object_type` | ✅ 1.1M |
| EVENT_SAMPLING_SITE | `p__sampling_site` | ✅ 1.1M |
| EVENT_SAMPLE_LOCATION | `p__sample_location` | ✅ 1.1M |
| EVENT_RESPONSIBILITY | `p__responsibility` | ✅ 1.1M |
| SITE_LOCATION | `p__site_location` | ✅ 18K |
| MSR_CURATION | `p__curation` | ❌ Not in OC |
| MSR_RELATED_RESOURCE | `p__related_resource` | ❌ Not in OC |
| EVENT_HAS_CONTEXT_CATEGORY | (shares `p__has_context_category`) | ❌ Not in OC |
| CURATION_RESPONSIBILITY | (shares `p__responsibility`) | ❌ Not in OC |

## Example Queries

### Wide Schema (simpler)
```sql
SELECT samp.pid, geo.latitude, geo.longitude
FROM pqg_wide AS samp
JOIN pqg_wide AS se ON se.row_id = ANY(samp.p__produced_by)
JOIN pqg_wide AS geo ON geo.row_id = ANY(se.p__sample_location)
WHERE samp.otype = 'MaterialSampleRecord'
```

### Narrow Schema (more joins)
```sql
SELECT samp.pid, geo.latitude, geo.longitude
FROM pqg AS samp
JOIN pqg AS e1 ON e1.s = samp.row_id AND e1.p = 'produced_by'
JOIN pqg AS se ON se.row_id = ANY(e1.o)
JOIN pqg AS e2 ON e2.s = se.row_id AND e2.p = 'sample_location'
JOIN pqg AS geo ON geo.row_id = ANY(e2.o)
WHERE samp.otype = 'MaterialSampleRecord' AND e1.otype = '_edge_' AND e2.otype = '_edge_'
```

## Next Steps

1. **TypedEdgeQueries dual-mode**: Add schema detection + wide query paths
2. **Conversion function**: Port Eric's `create_pqg_wide_table()` to pqg repo
3. **CLI commands**: `pqg convert --to-wide` / `--to-narrow`

## References

- Eric's code: `https://github.com/ekansa/open-context-py/.../isamples_pqg.py`
- PQG PR #6 (typed edges): `https://github.com/isamplesorg/pqg/pull/6` (still open)
- Plan file: `~/.claude/plans/parsed-yawning-knuth.md`
