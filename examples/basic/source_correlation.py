# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Cross-Source Correlation: Verifying and Enriching iSamples Data
#
# This notebook demonstrates how to use the iSamples source API clients to:
# 1. **Verify** data in parquet files against live source APIs
# 2. **Enrich** parquet records with additional metadata from sources
# 3. **Correlate** samples across different sources by location
#
# ## Available Source Clients
#
# | Client | Source | Domain |
# |--------|--------|--------|
# | `OpenContextClient` | opencontext.org | Archaeology |
# | `SESARClient` | geosamples.org | Geology (IGSN) |
# | `GEOMEClient` | geome-db.org | Genomics/Biology |
# | `SmithsonianClient` | si.edu | Museum Collections |

# %% [markdown]
# ## Setup

# %%
import os
import pandas as pd
import duckdb
from datetime import datetime

# iSamples source clients
from isamples_client.sources import (
    OpenContextClient,
    SESARClient,
    GEOMEClient,
    SmithsonianClient,
    SampleRecord,
)

# Parquet file paths - use local files if available, fallback to Cloudflare R2
import os
LOCAL_WIDE = os.path.expanduser("~/Data/iSample/pqg_refining/zenodo_wide_2026-01-09.parquet")
LOCAL_NARROW = os.path.expanduser("~/Data/iSample/pqg_refining/zenodo_narrow_2025-12-12.parquet")

# Use local files if they exist, otherwise use remote URLs
NARROW_URL = LOCAL_NARROW if os.path.exists(LOCAL_NARROW) else "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202512_narrow.parquet"
WIDE_URL = LOCAL_WIDE if os.path.exists(LOCAL_WIDE) else "https://pub-a18234d962364c22a50c787b7ca09fa5.r2.dev/isamples_202601_wide.parquet"

print(f"Using parquet: {WIDE_URL}")

# Initialize DuckDB connection
con = duckdb.connect()

# %% [markdown]
# ## 1. Explore Parquet Data by Source
#
# First, let's see what sources are in the parquet files and their sample counts.

# %%
# Count samples by source collection in the wide format
source_counts = con.sql(f"""
    SELECT
        COALESCE(n, 'UNKNOWN') as source,
        COUNT(*) as count
    FROM read_parquet('{WIDE_URL}')
    WHERE otype = 'MaterialSampleRecord'
    GROUP BY n
    ORDER BY count DESC
""").df()

print("Samples by source in parquet:")
print(source_counts.to_string(index=False))

# %% [markdown]
# ## 2. Verify OpenContext Records Against Live API
#
# Let's pick some OpenContext samples from the parquet and verify they exist in the live API.

# %%
# Get a sample of OpenContext records from parquet
oc_samples = con.sql(f"""
    SELECT
        pid as identifier,
        label,
        latitude,
        longitude,
        description
    FROM read_parquet('{WIDE_URL}')
    WHERE otype = 'MaterialSampleRecord'
      AND n = 'OPENCONTEXT'
      AND latitude IS NOT NULL
    LIMIT 5
""").df()

print("OpenContext samples from parquet:")
print(oc_samples[['identifier', 'label']].to_string(index=False))

# %%
# Verify these samples exist in the live OpenContext API
print("\nVerifying against live OpenContext API...\n")

with OpenContextClient() as oc:
    for _, row in oc_samples.iterrows():
        identifier = row['identifier']
        # Try to fetch the sample
        live_sample = oc.get_sample(identifier)

        if live_sample:
            match = "✓ MATCH" if live_sample.label == row['label'] else "~ LABEL DIFFERS"
            print(f"{match}: {identifier[:50]}")
            print(f"  Parquet: {row['label'][:50]}")
            print(f"  Live:    {live_sample.label[:50]}")
        else:
            print(f"✗ NOT FOUND: {identifier}")
        print()

# %% [markdown]
# ## 3. Verify SESAR Records (IGSN)
#
# SESAR samples are identified by IGSNs. Let's verify some against the live API.

# %%
# Get SESAR samples from parquet
sesar_samples = con.sql(f"""
    SELECT
        pid as identifier,
        label,
        latitude,
        longitude
    FROM read_parquet('{WIDE_URL}')
    WHERE otype = 'MaterialSampleRecord'
      AND n = 'SESAR'
      AND latitude IS NOT NULL
    LIMIT 5
""").df()

print("SESAR samples from parquet:")
print(sesar_samples[['identifier', 'label']].to_string(index=False))

# %%
# Verify against live SESAR API
print("\nVerifying against live SESAR API...\n")

with SESARClient() as sesar:
    for _, row in sesar_samples.iterrows():
        identifier = row['identifier']
        # Extract IGSN from identifier (may be in format "igsn:XXXXX" or URL)
        igsn = identifier.split('/')[-1].replace('igsn:', '').upper()

        live_sample = sesar.get_sample(igsn)

        if live_sample:
            print(f"✓ FOUND: {igsn}")
            print(f"  Parquet label: {row['label'][:50] if row['label'] else 'N/A'}")
            print(f"  Live label:    {live_sample.label[:50]}")
            if live_sample.material_type:
                print(f"  Material:      {live_sample.material_type}")
        else:
            print(f"✗ NOT FOUND: {igsn}")
        print()

# %% [markdown]
# ## 4. Enrich Parquet Data with Live API Details
#
# The parquet files contain core metadata, but source APIs often have additional details.
# Let's fetch enriched data for a sample.

# %%
def enrich_opencontext_sample(identifier: str) -> dict:
    """Fetch additional metadata from OpenContext API."""
    with OpenContextClient() as oc:
        sample = oc.get_sample(identifier)
        if sample:
            return {
                'identifier': identifier,
                'api_label': sample.label,
                'api_description': sample.description,
                'api_material': sample.material_type,
                'api_project': sample.project,
                'api_latitude': sample.latitude,
                'api_longitude': sample.longitude,
                'api_collection_date': sample.collection_date,
                'raw_fields': list(sample.raw_data.keys()) if sample.raw_data else [],
            }
    return None

# Example: enrich a single sample
if len(oc_samples) > 0:
    test_id = oc_samples.iloc[0]['identifier']
    enriched = enrich_opencontext_sample(test_id)
    if enriched:
        print("Enriched data from API:")
        for k, v in enriched.items():
            if k != 'raw_fields':
                print(f"  {k}: {v}")
        print(f"  Available raw fields: {len(enriched['raw_fields'])} fields")

# %% [markdown]
# ## 5. Cross-Source Location Correlation
#
# Find samples from different sources near the same geographic location.
# This is useful for discovering related samples across disciplines.

# %%
def find_samples_near_location(lat: float, lon: float, radius_km: float = 50) -> pd.DataFrame:
    """
    Find samples from ALL sources near a geographic point.

    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        radius_km: Search radius in kilometers

    Returns:
        DataFrame with samples from all sources
    """
    results = []

    # Query each source
    sources = [
        ('opencontext', OpenContextClient),
        ('geome', GEOMEClient),
        # SESAR doesn't have great geo search, skip for now
        # Smithsonian requires API key and has limited geo search
    ]

    for source_name, client_class in sources:
        try:
            with client_class() as client:
                for sample in client.get_samples_by_location(lat, lon, radius_km, max_results=10):
                    results.append({
                        'source': sample.source,
                        'identifier': sample.identifier[:60],
                        'label': sample.label[:50] if sample.label else 'N/A',
                        'latitude': sample.latitude,
                        'longitude': sample.longitude,
                        'material': sample.material_type,
                    })
        except Exception as e:
            print(f"Warning: {source_name} query failed: {e}")

    return pd.DataFrame(results)


# Example: Find samples near Mount Rainier (geology example location)
mt_rainier = (46.8523, -121.7603)
print(f"Searching for samples near Mount Rainier ({mt_rainier[0]}, {mt_rainier[1]})...\n")

nearby = find_samples_near_location(*mt_rainier, radius_km=100)
if len(nearby) > 0:
    print(f"Found {len(nearby)} samples from live APIs:")
    print(nearby.to_string(index=False))
else:
    print("No samples found in this area from live APIs.")

# %% [markdown]
# ## 6. Compare Parquet vs API for a Region
#
# Let's compare what's in the parquet files vs live APIs for a specific region.

# %%
# Pick a well-sampled region (Mediterranean/Greece area has lots of OpenContext data)
region = {
    'name': 'Eastern Mediterranean',
    'min_lat': 35.0,
    'max_lat': 42.0,
    'min_lon': 20.0,
    'max_lon': 35.0,
}

# Count samples in parquet for this region
parquet_count = con.sql(f"""
    SELECT
        n as source,
        COUNT(*) as parquet_count
    FROM read_parquet('{WIDE_URL}')
    WHERE otype = 'MaterialSampleRecord'
      AND latitude BETWEEN {region['min_lat']} AND {region['max_lat']}
      AND longitude BETWEEN {region['min_lon']} AND {region['max_lon']}
    GROUP BY n
    ORDER BY parquet_count DESC
""").df()

print(f"Samples in parquet for {region['name']}:")
print(parquet_count.to_string(index=False))

# %%
# Query live OpenContext API for the same region
print(f"\nQuerying live OpenContext API for {region['name']}...")

center_lat = (region['min_lat'] + region['max_lat']) / 2
center_lon = (region['min_lon'] + region['max_lon']) / 2

with OpenContextClient() as oc:
    # Use bbox search
    api_samples = list(oc.get_samples_by_location(
        center_lat, center_lon,
        radius_km=500,  # Large radius to cover region
        max_results=100
    ))

print(f"Live API returned {len(api_samples)} samples")
if api_samples:
    # Show sample of what's available
    print("\nSample of live API results:")
    for s in api_samples[:5]:
        print(f"  - {s.label[:50]}... ({s.identifier[:40]})")

# %% [markdown]
# ## 7. Identify Records Missing from Parquet
#
# Find records in live APIs that might be missing from the parquet export.

# %%
def find_missing_records(source: str, api_samples: list, parquet_url: str) -> list:
    """
    Compare API samples against parquet to find missing records.

    Args:
        source: Source name (e.g., 'OPENCONTEXT')
        api_samples: List of SampleRecord from API
        parquet_url: URL to parquet file

    Returns:
        List of identifiers in API but not in parquet
    """
    if not api_samples:
        return []

    # Get identifiers from API
    api_ids = [s.identifier for s in api_samples]

    # Query parquet for these identifiers
    ids_str = "', '".join(api_ids[:100])  # Limit to avoid huge query

    parquet_ids = con.sql(f"""
        SELECT pid
        FROM read_parquet('{parquet_url}')
        WHERE pid IN ('{ids_str}')
    """).df()['pid'].tolist()

    # Find missing
    missing = [id for id in api_ids if id not in parquet_ids]
    return missing


# Check for missing OpenContext records
if api_samples:
    missing = find_missing_records('OPENCONTEXT', api_samples[:50], WIDE_URL)
    print(f"Records in live API but not in parquet: {len(missing)}")
    if missing:
        print("Examples of missing records:")
        for m in missing[:5]:
            print(f"  - {m}")

# %% [markdown]
# ## 8. Batch Enrichment Example
#
# Enrich multiple parquet records with live API data.

# %%
def batch_enrich_samples(identifiers: list, source: str = 'opencontext') -> pd.DataFrame:
    """
    Batch enrich samples from a source API.

    Args:
        identifiers: List of sample identifiers
        source: Source name

    Returns:
        DataFrame with enriched data
    """
    enriched = []

    if source == 'opencontext':
        with OpenContextClient() as client:
            for i, identifier in enumerate(identifiers):
                if i % 10 == 0:
                    print(f"Processing {i+1}/{len(identifiers)}...")

                sample = client.get_sample(identifier)
                if sample:
                    enriched.append({
                        'identifier': identifier,
                        'label': sample.label,
                        'description': sample.description,
                        'material': sample.material_type,
                        'project': sample.project,
                        'lat': sample.latitude,
                        'lon': sample.longitude,
                    })

    return pd.DataFrame(enriched)


# Example: Enrich a small batch
sample_ids = oc_samples['identifier'].head(3).tolist()
if sample_ids:
    print("Batch enriching samples...\n")
    enriched_df = batch_enrich_samples(sample_ids)
    if len(enriched_df) > 0:
        print("Enriched data:")
        print(enriched_df.to_string(index=False))

# %% [markdown]
# ## Summary
#
# This notebook demonstrated:
#
# 1. **Verification**: Checking parquet records against live APIs
# 2. **Enrichment**: Fetching additional metadata not in parquet
# 3. **Cross-source correlation**: Finding related samples across sources
# 4. **Gap analysis**: Identifying records missing from parquet
#
# ### Use Cases
#
# - **Data Quality**: Verify parquet exports are complete and accurate
# - **Research**: Find related samples across disciplines (archaeology + geology)
# - **Updates**: Identify new records to add to parquet exports
# - **Enrichment**: Add detailed metadata from source APIs
#
# ### API Client Summary
#
# ```python
# from isamples_client.sources import (
#     OpenContextClient,  # Archaeology - no auth needed
#     SESARClient,        # Geology/IGSN - no auth for read
#     GEOMEClient,        # Genomics - no auth for read
#     SmithsonianClient,  # Museums - needs free API key
# )
# ```
