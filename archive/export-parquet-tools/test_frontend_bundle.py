"""Validation tests for the frontend bundle.

These tests verify data integrity and consistency across bundle files:
1. H3 cache sums match sample counts per source
2. Agent/site lookup joins return correct sample IDs
3. Manifest integrity (hash/row-count checks)
4. Summary file matches expected aggregations
"""

import hashlib
import json
import pytest
import duckdb
from pathlib import Path

# Bundle paths
BUNDLE_DIR = Path.home() / "Data/iSample/frontend_bundle_v2"
BUNDLE_V1_DIR = Path.home() / "Data/iSample/frontend_bundle"

# Source export for ground truth
EXPORT_PATH = Path.home() / "Data/iSample/2025_04_21_16_23_46/isamples_export_2025_04_21_16_23_46_geo.parquet"


def bundle_exists(version=2):
    """Check if bundle directory exists."""
    bundle_dir = BUNDLE_DIR if version == 2 else BUNDLE_V1_DIR
    return bundle_dir.exists()


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


@pytest.fixture
def con():
    """Create a DuckDB connection."""
    return duckdb.connect()


class TestManifestIntegrity:
    """Tests for manifest.json integrity."""

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_manifest_exists(self):
        """Manifest file should exist."""
        manifest_path = BUNDLE_DIR / "manifest.json"
        assert manifest_path.exists(), "manifest.json not found"

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_manifest_has_required_fields(self):
        """Manifest should have all required fields."""
        manifest_path = BUNDLE_DIR / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)

        required_fields = ["version", "generated", "source", "files", "h3_resolutions"]
        for field in required_fields:
            assert field in manifest, f"Missing required field: {field}"

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_file_hashes_match(self):
        """File hashes in manifest should match actual files."""
        manifest_path = BUNDLE_DIR / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)

        for name, info in manifest.get("files", {}).items():
            if "sha256" in info:
                file_path = BUNDLE_DIR / info["filename"]
                if file_path.exists():
                    actual_hash = compute_file_hash(file_path)
                    assert actual_hash == info["sha256"], \
                        f"Hash mismatch for {info['filename']}"

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_row_counts_match(self, con):
        """Row counts in manifest should match actual files."""
        manifest_path = BUNDLE_DIR / "manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)

        for name, info in manifest.get("files", {}).items():
            file_path = BUNDLE_DIR / info["filename"]
            if file_path.exists() and file_path.suffix == ".parquet":
                actual_count = con.sql(f"""
                    SELECT COUNT(*) FROM read_parquet('{file_path}')
                """).fetchone()[0]
                assert actual_count == info["rows"], \
                    f"Row count mismatch for {info['filename']}: manifest={info['rows']}, actual={actual_count}"


class TestH3CacheIntegrity:
    """Tests for H3 cache data integrity."""

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_h3_cache_sums_match_source_totals(self, con):
        """H3 cache sample counts per source should match export totals."""
        h3_cache_path = BUNDLE_DIR / "h3_cache.parquet"
        if not h3_cache_path.exists():
            pytest.skip("h3_cache.parquet not found")

        # Get source totals from H3 cache (pick one resolution)
        cache_totals = con.sql(f"""
            SELECT source_collection, SUM(sample_count) as total
            FROM read_parquet('{h3_cache_path}')
            WHERE resolution = 5
            GROUP BY source_collection
            ORDER BY source_collection
        """).fetchdf()

        # Get source totals from export (samples with coordinates and materials)
        export_totals = con.sql(f"""
            SELECT source_collection, COUNT(*) as total
            FROM read_parquet('{EXPORT_PATH}'),
            LATERAL (SELECT unnest(has_material_category) as mat)
            WHERE sample_location_latitude IS NOT NULL
              AND has_material_category IS NOT NULL
              AND len(has_material_category) > 0
            GROUP BY source_collection
            ORDER BY source_collection
        """).fetchdf()

        # Compare
        for _, row in cache_totals.iterrows():
            source = row['source_collection']
            cache_total = row['total']
            export_row = export_totals[export_totals['source_collection'] == source]
            if len(export_row) > 0:
                export_total = export_row['total'].values[0]
                assert cache_total == export_total, \
                    f"H3 cache sum mismatch for {source}: cache={cache_total}, export={export_total}"


class TestAgentLookupIntegrity:
    """Tests for agent lookup table integrity."""

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_agent_lookup_sample_ids_valid(self, con):
        """Agent lookup sample IDs should exist in main samples file."""
        agent_path = BUNDLE_DIR / "lookup_agents.parquet"
        combined_path = BUNDLE_DIR / "samples_frontend_combined.parquet"

        if not agent_path.exists() or not combined_path.exists():
            pytest.skip("Required files not found")

        # Get sample of agent sample_identifiers
        agent_samples = con.sql(f"""
            SELECT DISTINCT sample_identifier
            FROM read_parquet('{agent_path}')
            LIMIT 1000
        """).fetchdf()

        # Verify they exist in main file
        for sample_id in agent_samples['sample_identifier'].head(100):
            count = con.sql(f"""
                SELECT COUNT(*) FROM read_parquet('{combined_path}')
                WHERE sample_identifier = '{sample_id}'
            """).fetchone()[0]
            assert count > 0, f"Sample {sample_id} from agent lookup not found in samples"

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_agent_search_index_has_normalized_names(self, con):
        """Agent search index should have normalized (lowercase) names."""
        search_path = BUNDLE_DIR / "lookup_agents_search.parquet"
        if not search_path.exists():
            pytest.skip("lookup_agents_search.parquet not found")

        # Check that normalized names exist and are lowercase
        sample = con.sql(f"""
            SELECT agent_name, agent_name_normalized
            FROM read_parquet('{search_path}')
            LIMIT 100
        """).fetchdf()

        for _, row in sample.iterrows():
            if row['agent_name_normalized']:
                # Normalized should be lowercase
                assert row['agent_name_normalized'] == row['agent_name_normalized'].lower(), \
                    f"Normalized name not lowercase: {row['agent_name_normalized']}"


class TestSummaryFileIntegrity:
    """Tests for summary.parquet integrity."""

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_summary_file_small_enough(self):
        """Summary file should be under 5MB for instant load."""
        summary_path = BUNDLE_DIR / "summary.parquet"
        if not summary_path.exists():
            pytest.skip("summary.parquet not found")

        size_mb = summary_path.stat().st_size / 1e6
        assert size_mb < 5, f"Summary file too large: {size_mb:.1f} MB (target: <5 MB)"

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_summary_has_all_sources(self, con):
        """Summary should have data for all source collections."""
        summary_path = BUNDLE_DIR / "summary.parquet"
        if not summary_path.exists():
            pytest.skip("summary.parquet not found")

        summary_sources = con.sql(f"""
            SELECT DISTINCT source_collection
            FROM read_parquet('{summary_path}')
            ORDER BY source_collection
        """).fetchdf()

        export_sources = con.sql(f"""
            SELECT DISTINCT source_collection
            FROM read_parquet('{EXPORT_PATH}')
            WHERE sample_location_latitude IS NOT NULL
            ORDER BY source_collection
        """).fetchdf()

        summary_set = set(summary_sources['source_collection'])
        export_set = set(export_sources['source_collection'])

        assert summary_set == export_set, \
            f"Source mismatch: summary={summary_set}, export={export_set}"


class TestPartitionIntegrity:
    """Tests for partitioned samples files."""

    @pytest.mark.skipif(not bundle_exists(), reason="Bundle v2 not available")
    def test_partition_totals_match_combined(self, con):
        """Sum of partition row counts should match combined file."""
        partition_dir = BUNDLE_DIR / "samples_frontend"
        combined_path = BUNDLE_DIR / "samples_frontend_combined.parquet"

        if not partition_dir.exists() or not combined_path.exists():
            pytest.skip("Partition files not found")

        # Count rows in combined file
        combined_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{combined_path}')
        """).fetchone()[0]

        # Count rows across all partitions
        partition_count = con.sql(f"""
            SELECT COUNT(*) FROM read_parquet('{partition_dir}/*.parquet')
        """).fetchone()[0]

        assert combined_count == partition_count, \
            f"Partition total ({partition_count:,}) != combined ({combined_count:,})"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
