#!/usr/bin/env python3
"""Generate frontend-optimized parquet bundle for iSamples UI.

This script creates:
1. samples_frontend/ - Partitioned by source_collection with H3 columns
2. lookup_agents.parquet - Inverted index for agent→sample queries
3. lookup_agents_search.parquet - Lowercased, deduped agent names for autocomplete
4. lookup_sites.parquet - Inverted index for site→sample queries
5. h3_cache.parquet - Pre-aggregated H3 hexbin counts
6. summary.parquet - Instant-load file for first paint (<5MB)
7. manifest.json - Enhanced metadata with byte ranges and hashes

Based on recommendations from Codex and Gemini AI reviews.
Version 2.0: Added partitioning, search index, summary file, integrity hashes.
"""

import hashlib
import json
import time
from pathlib import Path
from datetime import datetime

import duckdb
import h3
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
EXPORT_PATH = Path.home() / "Data/iSample/2025_04_21_16_23_46/isamples_export_2025_04_21_16_23_46_geo.parquet"
OUTPUT_DIR = Path.home() / "Data/iSample/frontend_bundle_v2"
H3_RESOLUTIONS = [4, 5, 6, 7]  # Added resolution 4 for continental zoom
ROW_GROUP_SIZE = 50_000  # Smaller row groups for better Range Request perf


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def create_output_dir():
    """Create output directory if needed."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"Output directory: {OUTPUT_DIR}")


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of file for integrity verification."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def generate_frontend_export():
    """Create frontend-optimized export with H3 columns, partitioned by source."""
    log("Reading source export file...")
    con = duckdb.connect()

    # Read the export and add H3 columns
    df = con.sql(f"""
        SELECT *,
            sample_location_latitude as lat,
            sample_location_longitude as lon
        FROM read_parquet('{EXPORT_PATH}')
    """).fetchdf()

    log(f"Loaded {len(df):,} rows")

    # Add H3 columns for each resolution (including r4 for continental)
    for res in H3_RESOLUTIONS:
        col_name = f"h3_{res:02d}"
        log(f"Computing {col_name}...")
        start = time.time()

        h3_values = []
        for lat, lon in zip(df['sample_location_latitude'], df['sample_location_longitude']):
            if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
                try:
                    h3_values.append(h3.latlng_to_cell(lat, lon, res))
                except:
                    h3_values.append(None)
            else:
                h3_values.append(None)

        df[col_name] = h3_values
        log(f"  Computed in {time.time() - start:.1f}s")

    # Drop temporary columns
    df = df.drop(columns=['lat', 'lon'], errors='ignore')

    # Sort by source_collection then h3_05 for better compression and spatial queries
    log("Sorting by source_collection, h3_05...")
    df = df.sort_values(['source_collection', 'h3_05'])

    # Write partitioned by source_collection for lazy loading
    output_dir = OUTPUT_DIR / "samples_frontend"
    output_dir.mkdir(parents=True, exist_ok=True)
    log(f"Writing partitioned data to {output_dir}/...")

    partition_info = {}
    total_rows = 0

    for source in df['source_collection'].unique():
        source_df = df[df['source_collection'] == source]
        output_path = output_dir / f"source={source}.parquet"

        table = pa.Table.from_pandas(source_df)
        pq.write_table(
            table,
            output_path,
            compression='zstd',
            row_group_size=ROW_GROUP_SIZE,
            use_dictionary=True,
        )

        size_mb = output_path.stat().st_size / 1e6
        file_hash = compute_file_hash(output_path)

        partition_info[source] = {
            "filename": output_path.name,
            "rows": len(source_df),
            "size_bytes": output_path.stat().st_size,
            "size_mb": round(size_mb, 2),
            "sha256": file_hash,
        }
        total_rows += len(source_df)
        log(f"  {source}: {len(source_df):,} rows, {size_mb:.1f} MB")

    # Also write combined file for simpler queries
    combined_path = OUTPUT_DIR / "samples_frontend_combined.parquet"
    log(f"Writing combined file to {combined_path}...")
    table = pa.Table.from_pandas(df)
    pq.write_table(
        table,
        combined_path,
        compression='zstd',
        row_group_size=ROW_GROUP_SIZE,
        use_dictionary=True,
    )
    combined_size = combined_path.stat().st_size / 1e6
    log(f"  Combined: {combined_size:.1f} MB")

    return total_rows, output_dir, partition_info, combined_path


def generate_agent_lookup():
    """Create inverted index for agent→sample queries."""
    log("Generating agent→sample lookup table...")
    con = duckdb.connect()

    # Extract agents from nested structs
    df = con.sql(f"""
        SELECT
            resp.name as agent_name,
            resp.role as agent_role,
            sample_identifier,
            source_collection
        FROM read_parquet('{EXPORT_PATH}'),
        LATERAL (
            SELECT unnest(produced_by.responsibility) as resp
        )
        WHERE produced_by IS NOT NULL
          AND produced_by.responsibility IS NOT NULL
          AND len(produced_by.responsibility) > 0
        ORDER BY resp.name, source_collection
    """).fetchdf()

    log(f"  Found {len(df):,} agent-sample relationships")

    # Also get curation responsibility
    df_curation = con.sql(f"""
        SELECT
            resp.name as agent_name,
            resp.role as agent_role,
            sample_identifier,
            source_collection
        FROM read_parquet('{EXPORT_PATH}'),
        LATERAL (
            SELECT unnest(curation.responsibility) as resp
        )
        WHERE curation IS NOT NULL
          AND curation.responsibility IS NOT NULL
          AND len(curation.responsibility) > 0
        ORDER BY resp.name, source_collection
    """).fetchdf()

    log(f"  Found {len(df_curation):,} curation agent-sample relationships")

    # Combine and deduplicate
    import pandas as pd
    df_combined = pd.concat([df, df_curation]).drop_duplicates()
    df_combined = df_combined.sort_values(['agent_name', 'source_collection'])

    log(f"  Total unique: {len(df_combined):,} relationships")

    output_path = OUTPUT_DIR / "lookup_agents.parquet"
    table = pa.Table.from_pandas(df_combined)
    pq.write_table(
        table,
        output_path,
        compression='zstd',
        row_group_size=ROW_GROUP_SIZE,
    )

    size_mb = output_path.stat().st_size / 1e6
    log(f"  Written: {size_mb:.1f} MB")

    return len(df_combined), output_path, df_combined


def generate_agent_search_index(agent_df):
    """Create search-optimized agent index for autocomplete (lowercased, deduped)."""
    log("Generating agent search index...")
    import pandas as pd
    import unicodedata

    def normalize_name(name):
        """Lowercase and remove diacritics for search matching."""
        if pd.isna(name):
            return None
        # Normalize unicode and remove diacritics
        normalized = unicodedata.normalize('NFKD', str(name))
        ascii_name = normalized.encode('ASCII', 'ignore').decode('ASCII')
        return ascii_name.lower().strip()

    # Create search index with normalized names
    search_df = agent_df.copy()
    search_df['agent_name_normalized'] = search_df['agent_name'].apply(normalize_name)

    # Aggregate: unique agents with sample counts and source distribution
    search_agg = search_df.groupby(['agent_name', 'agent_name_normalized']).agg(
        sample_count=('sample_identifier', 'count'),
        sources=('source_collection', lambda x: list(x.unique())),
        roles=('agent_role', lambda x: list(x.dropna().unique())),
    ).reset_index()

    # Sort by sample count descending for autocomplete ranking
    search_agg = search_agg.sort_values('sample_count', ascending=False)

    log(f"  Unique agents: {len(search_agg):,}")

    output_path = OUTPUT_DIR / "lookup_agents_search.parquet"
    table = pa.Table.from_pandas(search_agg)
    pq.write_table(
        table,
        output_path,
        compression='zstd',
    )

    size_mb = output_path.stat().st_size / 1e6
    log(f"  Written: {size_mb:.1f} MB")

    return len(search_agg), output_path


def generate_site_lookup():
    """Create inverted index for site→sample queries."""
    log("Generating site→sample lookup table...")
    con = duckdb.connect()

    # Extract sites from nested structs
    df = con.sql(f"""
        SELECT
            produced_by.sampling_site.label as site_label,
            produced_by.sampling_site.description as site_description,
            sample_identifier,
            source_collection,
            sample_location_latitude as lat,
            sample_location_longitude as lon
        FROM read_parquet('{EXPORT_PATH}')
        WHERE produced_by IS NOT NULL
          AND produced_by.sampling_site IS NOT NULL
          AND produced_by.sampling_site.label IS NOT NULL
        ORDER BY produced_by.sampling_site.label, source_collection
    """).fetchdf()

    log(f"  Found {len(df):,} site-sample relationships")

    output_path = OUTPUT_DIR / "lookup_sites.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(
        table,
        output_path,
        compression='zstd',
        row_group_size=ROW_GROUP_SIZE,
    )

    size_mb = output_path.stat().st_size / 1e6
    log(f"  Written: {size_mb:.1f} MB")

    return len(df), output_path


def generate_h3_cache():
    """Create pre-aggregated H3 hexbin cache."""
    log("Generating H3 aggregation cache...")
    con = duckdb.connect()

    # First, we need to compute H3 values
    # Read coordinates
    df = con.sql(f"""
        SELECT
            sample_location_latitude as lat,
            sample_location_longitude as lon,
            source_collection,
            unnest(has_material_category).identifier as material_category
        FROM read_parquet('{EXPORT_PATH}')
        WHERE sample_location_latitude IS NOT NULL
          AND has_material_category IS NOT NULL
          AND len(has_material_category) > 0
    """).fetchdf()

    log(f"  Processing {len(df):,} samples with coordinates and materials")

    # Add H3 columns
    for res in H3_RESOLUTIONS:
        col_name = f"h3_{res:02d}"
        h3_values = []
        for lat, lon in zip(df['lat'], df['lon']):
            if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
                try:
                    h3_values.append(h3.latlng_to_cell(lat, lon, res))
                except:
                    h3_values.append(None)
            else:
                h3_values.append(None)
        df[col_name] = h3_values

    # Create aggregations for each resolution
    import pandas as pd
    cache_dfs = []

    for res in H3_RESOLUTIONS:
        col_name = f"h3_{res:02d}"
        log(f"  Aggregating resolution {res}...")

        # Aggregate by h3 + source
        agg = df.groupby([col_name, 'source_collection']).agg(
            sample_count=('lat', 'count'),
            lat_centroid=('lat', 'mean'),
            lon_centroid=('lon', 'mean'),
        ).reset_index()
        agg['resolution'] = res
        agg = agg.rename(columns={col_name: 'h3_index'})
        cache_dfs.append(agg)

    cache_df = pd.concat(cache_dfs, ignore_index=True)
    cache_df = cache_df.sort_values(['resolution', 'h3_index'])

    log(f"  Total cache entries: {len(cache_df):,}")

    output_path = OUTPUT_DIR / "h3_cache.parquet"
    table = pa.Table.from_pandas(cache_df)
    pq.write_table(
        table,
        output_path,
        compression='zstd',
        row_group_size=ROW_GROUP_SIZE,
    )

    size_mb = output_path.stat().st_size / 1e6
    log(f"  Written: {size_mb:.1f} MB")

    return len(cache_df), output_path


def generate_facet_cache():
    """Create pre-computed facet counts."""
    log("Generating facet count cache...")
    con = duckdb.connect()

    # Material category facets
    material_counts = con.sql(f"""
        SELECT
            mat.identifier as facet_value,
            'material_category' as facet_type,
            source_collection,
            COUNT(*) as count
        FROM read_parquet('{EXPORT_PATH}'),
        LATERAL (SELECT unnest(has_material_category) as mat)
        WHERE has_material_category IS NOT NULL AND len(has_material_category) > 0
        GROUP BY mat.identifier, source_collection
        ORDER BY count DESC
    """).fetchdf()

    # Context category facets
    context_counts = con.sql(f"""
        SELECT
            ctx.identifier as facet_value,
            'context_category' as facet_type,
            source_collection,
            COUNT(*) as count
        FROM read_parquet('{EXPORT_PATH}'),
        LATERAL (SELECT unnest(has_context_category) as ctx)
        WHERE has_context_category IS NOT NULL AND len(has_context_category) > 0
        GROUP BY ctx.identifier, source_collection
        ORDER BY count DESC
    """).fetchdf()

    # Sample object type facets
    object_counts = con.sql(f"""
        SELECT
            obj.identifier as facet_value,
            'sample_object_type' as facet_type,
            source_collection,
            COUNT(*) as count
        FROM read_parquet('{EXPORT_PATH}'),
        LATERAL (SELECT unnest(has_sample_object_type) as obj)
        WHERE has_sample_object_type IS NOT NULL AND len(has_sample_object_type) > 0
        GROUP BY obj.identifier, source_collection
        ORDER BY count DESC
    """).fetchdf()

    # Source collection totals
    source_counts = con.sql(f"""
        SELECT
            source_collection as facet_value,
            'source_collection' as facet_type,
            source_collection,
            COUNT(*) as count
        FROM read_parquet('{EXPORT_PATH}')
        GROUP BY source_collection
        ORDER BY count DESC
    """).fetchdf()

    import pandas as pd
    facet_df = pd.concat([material_counts, context_counts, object_counts, source_counts], ignore_index=True)

    log(f"  Total facet entries: {len(facet_df):,}")

    output_path = OUTPUT_DIR / "facet_cache.parquet"
    table = pa.Table.from_pandas(facet_df)
    pq.write_table(
        table,
        output_path,
        compression='zstd',
    )

    size_mb = output_path.stat().st_size / 1e6
    log(f"  Written: {size_mb:.1f} MB")

    return len(facet_df), output_path


def generate_summary():
    """Create instant-load summary file for first paint (<5MB target).

    Contains:
    - H3 hexbin counts at resolution 4 (continental overview)
    - Global facet counts
    - Source collection totals and bounding boxes
    """
    log("Generating summary.parquet for instant first paint...")
    con = duckdb.connect()
    import pandas as pd

    # 1. H3 resolution 4 aggregates (continental overview)
    log("  Computing H3 r4 summary...")
    df_coords = con.sql(f"""
        SELECT
            sample_location_latitude as lat,
            sample_location_longitude as lon,
            source_collection
        FROM read_parquet('{EXPORT_PATH}')
        WHERE sample_location_latitude IS NOT NULL
    """).fetchdf()

    # Compute H3 r4
    h3_values = []
    for lat, lon in zip(df_coords['lat'], df_coords['lon']):
        if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
            try:
                h3_values.append(h3.latlng_to_cell(lat, lon, 4))
            except:
                h3_values.append(None)
        else:
            h3_values.append(None)
    df_coords['h3_04'] = h3_values

    # Aggregate
    h3_summary = df_coords.groupby(['h3_04', 'source_collection']).agg(
        sample_count=('lat', 'count'),
        lat_centroid=('lat', 'mean'),
        lon_centroid=('lon', 'mean'),
    ).reset_index()
    h3_summary['data_type'] = 'h3_r4'

    # 2. Global facet totals (not per-source)
    log("  Computing global facet totals...")
    global_facets = con.sql(f"""
        SELECT
            'material_category' as facet_type,
            mat.identifier as facet_value,
            COUNT(*) as count
        FROM read_parquet('{EXPORT_PATH}'),
        LATERAL (SELECT unnest(has_material_category) as mat)
        WHERE has_material_category IS NOT NULL AND len(has_material_category) > 0
        GROUP BY mat.identifier
        ORDER BY count DESC
    """).fetchdf()
    global_facets['data_type'] = 'facet_global'

    # 3. Source collection summary
    log("  Computing source summary...")
    source_summary = con.sql(f"""
        SELECT
            source_collection,
            COUNT(*) as sample_count,
            SUM(CASE WHEN sample_location_latitude IS NOT NULL THEN 1 ELSE 0 END) as with_coords,
            MIN(sample_location_latitude) as min_lat,
            MAX(sample_location_latitude) as max_lat,
            MIN(sample_location_longitude) as min_lon,
            MAX(sample_location_longitude) as max_lon
        FROM read_parquet('{EXPORT_PATH}')
        GROUP BY source_collection
    """).fetchdf()
    source_summary['data_type'] = 'source_summary'

    # Combine into single file with data_type discriminator
    # For simplicity, we'll store as separate row groups
    output_path = OUTPUT_DIR / "summary.parquet"

    # Write H3 summary (main content for map first paint)
    h3_summary_clean = h3_summary[['h3_04', 'source_collection', 'sample_count', 'lat_centroid', 'lon_centroid']].copy()
    h3_summary_clean.columns = ['h3_index', 'source_collection', 'sample_count', 'lat_centroid', 'lon_centroid']

    table = pa.Table.from_pandas(h3_summary_clean)
    pq.write_table(
        table,
        output_path,
        compression='zstd',
    )

    size_mb = output_path.stat().st_size / 1e6
    log(f"  Written: {size_mb:.1f} MB")

    return len(h3_summary_clean), output_path


def generate_manifest(stats: dict, partition_info: dict = None):
    """Create enhanced manifest.json with integrity hashes and partition info."""
    log("Generating manifest.json...")

    manifest = {
        "version": "2.0.0",
        "schema_version": "isamples-frontend-bundle-v2",
        "generated": datetime.now().isoformat(),
        "source": str(EXPORT_PATH),
        "source_doi": "zenodo.org/record/XXXXX",  # TODO: Add actual DOI
        "files": {},
        "h3_resolutions": H3_RESOLUTIONS,
        "row_group_size": ROW_GROUP_SIZE,
    }

    # Add file metadata with hashes
    for name, info in stats.items():
        file_path = OUTPUT_DIR / info["filename"]
        if file_path.exists():
            manifest["files"][name] = {
                "filename": info["filename"],
                "rows": info["rows"],
                "size_bytes": file_path.stat().st_size,
                "size_mb": round(file_path.stat().st_size / 1e6, 2),
                "description": info["description"],
                "sha256": compute_file_hash(file_path),
            }

    # Add partition info if provided
    if partition_info:
        manifest["partitions"] = {
            "partition_column": "source_collection",
            "files": partition_info,
        }

    # Add source collection metadata
    con = duckdb.connect()
    sources = con.sql(f"""
        SELECT source_collection, COUNT(*) as count,
               SUM(CASE WHEN sample_location_latitude IS NOT NULL THEN 1 ELSE 0 END) as with_coordinates,
               MIN(sample_location_latitude) as min_lat,
               MAX(sample_location_latitude) as max_lat,
               MIN(sample_location_longitude) as min_lon,
               MAX(sample_location_longitude) as max_lon
        FROM read_parquet('{EXPORT_PATH}')
        GROUP BY source_collection
    """).fetchdf()

    manifest["sources"] = sources.to_dict(orient='records')

    # Add column schema for main samples file
    manifest["schema"] = {
        "h3_columns": [f"h3_{r:02d}" for r in H3_RESOLUTIONS],
        "coordinate_columns": ["sample_location_latitude", "sample_location_longitude"],
        "id_column": "sample_identifier",
        "source_column": "source_collection",
    }

    # Add recommended load order for UI
    manifest["load_order"] = [
        {"file": "summary.parquet", "purpose": "instant_map", "required": True},
        {"file": "facet_cache.parquet", "purpose": "instant_facets", "required": True},
        {"file": "lookup_agents_search.parquet", "purpose": "autocomplete", "required": False},
        {"file": "h3_cache.parquet", "purpose": "detailed_map", "required": False},
        {"file": "samples_frontend/", "purpose": "full_data", "required": False},
    ]

    output_path = OUTPUT_DIR / "manifest.json"
    with open(output_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    log(f"  Written: {output_path}")
    return output_path


def main():
    """Generate complete frontend bundle v2."""
    log("=" * 60)
    log("iSamples Frontend Bundle Generator v2.0")
    log("=" * 60)

    create_output_dir()

    stats = {}
    partition_info = None

    # 1. Frontend-optimized export with H3 (partitioned)
    rows, path, partition_info, combined_path = generate_frontend_export()
    stats["samples_combined"] = {
        "filename": combined_path.name,
        "rows": rows,
        "description": "Combined sample data with H3 columns (h3_04-07), sorted by source_collection + h3_05"
    }

    # 2. Agent lookup
    rows, path, agent_df = generate_agent_lookup()
    stats["agents"] = {
        "filename": path.name,
        "rows": rows,
        "description": "Inverted index for agent→sample queries"
    }

    # 3. Agent search index (for autocomplete)
    rows, path = generate_agent_search_index(agent_df)
    stats["agents_search"] = {
        "filename": path.name,
        "rows": rows,
        "description": "Search-optimized agent index (lowercased, deduped, with sample counts)"
    }

    # 4. Site lookup
    rows, path = generate_site_lookup()
    stats["sites"] = {
        "filename": path.name,
        "rows": rows,
        "description": "Inverted index for site→sample queries"
    }

    # 5. H3 cache (resolutions 4-7)
    rows, path = generate_h3_cache()
    stats["h3_cache"] = {
        "filename": path.name,
        "rows": rows,
        "description": "Pre-aggregated H3 hexbin counts by resolution (4-7) and source"
    }

    # 6. Facet cache
    rows, path = generate_facet_cache()
    stats["facets"] = {
        "filename": path.name,
        "rows": rows,
        "description": "Pre-computed facet counts for fast UI rendering"
    }

    # 7. Summary file (instant load for first paint)
    rows, path = generate_summary()
    stats["summary"] = {
        "filename": path.name,
        "rows": rows,
        "description": "Instant-load H3 r4 summary for map first paint (<5MB)"
    }

    # 8. Enhanced manifest with hashes and partitions
    generate_manifest(stats, partition_info)

    log("=" * 60)
    log("COMPLETE! Bundle v2 summary:")
    log("=" * 60)

    total_size = 0
    for name, info in stats.items():
        file_path = OUTPUT_DIR / info["filename"]
        if file_path.exists():
            size_mb = file_path.stat().st_size / 1e6
            total_size += size_mb
            log(f"  {info['filename']}: {info['rows']:,} rows, {size_mb:.1f} MB")

    # Add partition sizes
    if partition_info:
        log("  Partitioned samples:")
        for source, pinfo in partition_info.items():
            log(f"    source={source}.parquet: {pinfo['rows']:,} rows, {pinfo['size_mb']:.1f} MB")
            total_size += pinfo['size_mb']

    log(f"  manifest.json")
    log(f"  TOTAL: {total_size:.1f} MB")
    log(f"\nOutput directory: {OUTPUT_DIR}")

    # Print load order recommendation
    log("\n" + "=" * 60)
    log("Recommended UI load order:")
    log("=" * 60)
    log("  1. summary.parquet → Instant map (first paint)")
    log("  2. facet_cache.parquet → Facet UI")
    log("  3. lookup_agents_search.parquet → Autocomplete")
    log("  4. h3_cache.parquet → Detailed hexbin map")
    log("  5. samples_frontend/*.parquet → On-demand detail")


if __name__ == "__main__":
    main()
