# Export Parquet Tools (Archived)

Tools for processing iSamples **export format** parquet files.

## What's Here

| File | Purpose |
|------|---------|
| `generate_frontend_bundle.py` | Creates frontend-optimized bundles (H3 cache, lookup tables, partitioned data) |
| `test_frontend_bundle.py` | Validation tests for the bundle |

## Data Format

These tools work with the **export parquet format** - the original sample-centric format with nested structs:

```
~/Data/iSample/2025_04_21_16_23_46/isamples_export_2025_04_21_16_23_46_geo.parquet
```

This is different from the current **wide/narrow PQG formats** on Cloudflare R2.

## Why Archived

1. **Format mismatch**: Current workflows use wide/narrow parquet, not export format
2. **Known issues**: Codex review identified bugs (inflated H3 counts, incomplete summary output)
3. **Scope change**: Repo pivoted to examples-only; data processing scripts are out of scope

## If You Need This

If you have export-format parquet files and need frontend bundles:

1. Review and fix the known issues (see PR #2 Codex review)
2. Update `EXPORT_PATH` in `generate_frontend_bundle.py` to your file
3. Run: `python generate_frontend_bundle.py`

## Known Issues (from Codex Review)

- H3 cache counts inflated by material category multiplicity (lines 302-344)
- Summary file only writes H3 data, not global facets/source summary (lines 446-538)
- Coordinate filtering inconsistent (lat vs lon null checks)
- No filename sanitization for source_collection partitions
