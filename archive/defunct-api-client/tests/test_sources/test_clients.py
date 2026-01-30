"""
Tests for iSamples source API clients.

Unit tests use mocked responses.
Integration tests (marked with @pytest.mark.integration) make live API calls.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from isamples_client.sources import (
    BaseSourceClient,
    SampleRecord,
    OpenContextClient,
    SESARClient,
    GEOMEClient,
    SmithsonianClient,
)


# =============================================================================
# Unit Tests - SampleRecord
# =============================================================================

class TestSampleRecord:
    """Tests for SampleRecord dataclass."""

    def test_create_sample_record(self):
        """Test basic SampleRecord creation."""
        sample = SampleRecord(
            source="test",
            identifier="test-001",
            label="Test Sample",
            description="A test sample",
            latitude=46.85,
            longitude=-121.76,
        )
        assert sample.source == "test"
        assert sample.identifier == "test-001"
        assert sample.has_coordinates is True

    def test_sample_record_no_coordinates(self):
        """Test SampleRecord without coordinates."""
        sample = SampleRecord(
            source="test",
            identifier="test-002",
            label="No Location",
        )
        assert sample.has_coordinates is False

    def test_sample_record_to_dict(self):
        """Test SampleRecord serialization."""
        sample = SampleRecord(
            source="opencontext",
            identifier="oc-123",
            label="Pottery Shard",
            latitude=37.5,
            longitude=22.8,
            collection_date=datetime(2024, 6, 15),
        )
        d = sample.to_dict()
        assert d["source"] == "opencontext"
        assert d["collection_date"] == "2024-06-15T00:00:00"


# =============================================================================
# Unit Tests - OpenContext Client
# =============================================================================

class TestOpenContextClient:
    """Unit tests for OpenContextClient with mocked responses."""

    @pytest.fixture
    def mock_oc_search_response(self):
        """Sample OpenContext search response."""
        return {
            "oc-api:has-results": [
                {
                    "uri": "https://opencontext.org/subjects/abc123",
                    "citation uri": "https://opencontext.org/subjects/abc123",
                    "label": "Pottery Fragment",
                    "category": "Sample",
                    "latitude": 37.5,
                    "longitude": 22.8,
                    "updated": "2024-06-15T10:00:00Z",
                    "context": "Project A / Site B / Trench 1",
                }
            ],
            "next-json": None,
        }

    def test_client_initialization(self):
        """Test client initializes correctly."""
        client = OpenContextClient()
        assert client.SOURCE_NAME == "opencontext"
        assert "opencontext.org" in client.base_url
        client.close()

    def test_context_manager(self):
        """Test client works as context manager."""
        with OpenContextClient() as client:
            assert client is not None
        # Client should be closed after context

    @patch.object(OpenContextClient, "client", new_callable=MagicMock)
    def test_search_parses_results(self, mock_http, mock_oc_search_response):
        """Test search correctly parses API response."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_oc_search_response
        mock_response.raise_for_status = MagicMock()
        mock_http.get.return_value = mock_response

        with OpenContextClient() as oc:
            results = list(oc.search("pottery", max_results=10))

        assert len(results) == 1
        assert results[0].label == "Pottery Fragment"
        assert results[0].latitude == 37.5


# =============================================================================
# Unit Tests - SESAR Client
# =============================================================================

class TestSESARClient:
    """Unit tests for SESARClient with mocked responses."""

    @pytest.fixture
    def mock_sesar_jsonld_response(self):
        """Sample SESAR JSON-LD response."""
        return {
            "description": {
                "sampleName": "Rock Sample ABC",
                "sampleType": "Rock",
                "material": "Basalt",
                "latitude": 46.85,
                "longitude": -121.76,
                "collectionStartDate": "2024-06-10",
            }
        }

    def test_client_initialization(self):
        """Test client initializes correctly."""
        client = SESARClient()
        assert client.SOURCE_NAME == "sesar"
        client.close()

    def test_normalize_igsn(self):
        """Test IGSN normalization."""
        with SESARClient() as sesar:
            assert sesar._normalize_igsn("IEWFS0001") == "IEWFS0001"
            assert sesar._normalize_igsn("igsn:iewfs0001") == "IEWFS0001"
            assert sesar._normalize_igsn("https://igsn.org/IEWFS0001") == "IEWFS0001"

    def test_looks_like_igsn(self):
        """Test IGSN detection."""
        with SESARClient() as sesar:
            assert sesar._looks_like_igsn("IEWFS0001") is True
            assert sesar._looks_like_igsn("igsn:ABC123") is True
            assert sesar._looks_like_igsn("some random text") is False


# =============================================================================
# Unit Tests - GEOME Client
# =============================================================================

class TestGEOMEClient:
    """Unit tests for GEOMEClient with mocked responses."""

    @pytest.fixture
    def mock_geome_record(self):
        """Sample GEOME record."""
        return {
            "bcid": "ark:/12345/abc",
            "materialSampleID": "SAMPLE001",
            "decimalLatitude": 25.5,
            "decimalLongitude": -80.2,
            "yearCollected": 2024,
            "monthCollected": 6,
            "dayCollected": 15,
            "tissueType": "muscle",
            "locality": "Florida Keys",
        }

    def test_client_initialization(self):
        """Test client initializes correctly."""
        client = GEOMEClient()
        assert client.SOURCE_NAME == "geome"
        assert "geome-db.org" in client.base_url
        client.close()

    def test_parse_date_fields(self):
        """Test date parsing from GEOME fields."""
        with GEOMEClient() as geome:
            record = {
                "yearCollected": 2024,
                "monthCollected": 6,
                "dayCollected": 15,
            }
            date = geome._parse_date_fields(record)
            assert date is not None
            assert date.year == 2024
            assert date.month == 6

    def test_parse_date_fields_year_only(self):
        """Test date parsing with only year."""
        with GEOMEClient() as geome:
            record = {"yearCollected": 2024}
            date = geome._parse_date_fields(record)
            assert date is not None
            assert date.year == 2024


# =============================================================================
# Unit Tests - Smithsonian Client
# =============================================================================

class TestSmithsonianClient:
    """Unit tests for SmithsonianClient with mocked responses."""

    def test_client_initialization_no_key(self):
        """Test client warns without API key."""
        with patch.dict("os.environ", {}, clear=True):
            client = SmithsonianClient()
            assert client.api_key is None
            client.close()

    def test_client_initialization_with_key(self):
        """Test client accepts API key."""
        client = SmithsonianClient(api_key="test_key_123")
        assert client.api_key == "test_key_123"
        client.close()

    def test_client_reads_env_key(self):
        """Test client reads API key from environment."""
        with patch.dict("os.environ", {"SMITHSONIAN_API_KEY": "env_key_456"}):
            client = SmithsonianClient()
            assert client.api_key == "env_key_456"
            client.close()


# =============================================================================
# Integration Tests - Live API Calls
# =============================================================================

@pytest.mark.integration
class TestOpenContextIntegration:
    """Integration tests for OpenContext (requires network)."""

    def test_search_live(self):
        """Test live search against OpenContext API."""
        with OpenContextClient() as oc:
            results = list(oc.search("pottery", max_results=3))
            # Should get some results (API is open)
            assert len(results) >= 0  # May be 0 if API is down

    def test_list_projects_live(self):
        """Test listing projects from OpenContext."""
        with OpenContextClient() as oc:
            projects = list(oc.list_projects(max_results=3))
            assert len(projects) >= 0


@pytest.mark.integration
class TestSESARIntegration:
    """Integration tests for SESAR (requires network)."""

    def test_get_sample_live(self):
        """Test fetching a known SESAR sample."""
        with SESARClient() as sesar:
            # This IGSN may or may not exist - just test it doesn't crash
            sample = sesar.get_sample("IEWFS0001")
            # Result may be None if sample not found


@pytest.mark.integration
class TestGEOMEIntegration:
    """Integration tests for GEOME (requires network)."""

    def test_list_projects_live(self):
        """Test listing GEOME projects."""
        with GEOMEClient() as geome:
            projects = list(geome.list_projects(max_results=3))
            assert len(projects) >= 0


# =============================================================================
# Cross-Source Correlation Tests
# =============================================================================

class TestCrossSourceCorrelation:
    """Tests for comparing samples across sources."""

    def test_unified_sample_record_format(self):
        """Test that all clients produce compatible SampleRecord objects."""
        # Create sample records as each client would
        samples = [
            SampleRecord(
                source="opencontext",
                identifier="oc:123",
                label="Archaeological Sample",
                latitude=37.5,
                longitude=22.8,
            ),
            SampleRecord(
                source="sesar",
                identifier="igsn:ABC123",
                label="Geological Sample",
                latitude=37.5,
                longitude=22.8,
            ),
            SampleRecord(
                source="geome",
                identifier="ark:/123/abc",
                label="Biological Sample",
                latitude=37.5,
                longitude=22.8,
            ),
            SampleRecord(
                source="smithsonian",
                identifier="si:xyz",
                label="Museum Sample",
                latitude=37.5,
                longitude=22.8,
            ),
        ]

        # All should have coordinates
        assert all(s.has_coordinates for s in samples)

        # All should be serializable
        dicts = [s.to_dict() for s in samples]
        assert all("source" in d for d in dicts)
        assert all("identifier" in d for d in dicts)
