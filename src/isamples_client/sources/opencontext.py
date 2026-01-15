"""
OpenContext API client for accessing archaeological sample data.

OpenContext (https://opencontext.org) provides open access to archaeological
research data with a read-only JSON-LD API.

API Documentation: https://opencontext.org/about/services
"""

import logging
from datetime import datetime
from typing import Any, Iterator, Optional

import dateparser

from .base import BaseSourceClient, SampleRecord

logger = logging.getLogger(__name__)


class OpenContextClient(BaseSourceClient):
    """
    Client for the OpenContext archaeological data API.

    OpenContext provides rich archaeological sample data including
    artifacts, ecofacts, and excavation samples with geographic
    and temporal metadata.

    Example:
        >>> with OpenContextClient() as oc:
        ...     for sample in oc.search("pottery", max_results=10):
        ...         print(f"{sample.identifier}: {sample.label}")

    Note:
        OpenContext is read-only and does not require authentication.
        A proper User-Agent header is required to avoid being blocked.
    """

    SOURCE_NAME = "opencontext"
    BASE_URL = "https://opencontext.org"

    # Default query parameters for comprehensive results
    DEFAULT_PARAMS = {
        "add-attribute-uris": "1",
        "response": "metadata,uri-meta",
        "sort": "updated--desc",
    }

    def __init__(
        self,
        timeout: float = 30.0,
        user_agent: str = "isamples-client/1.0 (opencontext)",
    ):
        """
        Initialize OpenContext client.

        Args:
            timeout: HTTP request timeout in seconds
            user_agent: User-Agent header (required by OpenContext)
        """
        super().__init__(
            base_url=self.BASE_URL,
            timeout=timeout,
            user_agent=user_agent,
        )

    def search(
        self,
        query: str,
        max_results: int = 100,
        record_type: str = "subjects",
        **kwargs: Any,
    ) -> Iterator[SampleRecord]:
        """
        Search OpenContext for samples matching a query.

        Args:
            query: Full-text search query
            max_results: Maximum number of results to return
            record_type: Type of records ('subjects', 'media', 'projects')
            **kwargs: Additional query parameters

        Yields:
            SampleRecord objects matching the query
        """
        url = f"{self.base_url}/query/.json"
        params = {
            **self.DEFAULT_PARAMS,
            "q": query,
            "type": record_type,
            "rows": min(max_results, 100),  # API max is typically 100 per page
            **kwargs,
        }

        count = 0
        while url and count < max_results:
            logger.debug(f"Fetching OpenContext: {url}")
            response = self.client.get(url, params=params if count == 0 else None)
            response.raise_for_status()

            data = response.json()
            results = data.get("oc-api:has-results", [])

            if not results:
                break

            for record in results:
                if count >= max_results:
                    break
                sample = self._parse_record(record)
                if sample:
                    yield sample
                    count += 1

            # Get next page URL
            url = data.get("next-json")
            params = None  # Next URL includes params

    def get_sample(self, identifier: str) -> Optional[SampleRecord]:
        """
        Fetch a single sample by its OpenContext URI or UUID.

        Args:
            identifier: OpenContext URI (e.g., "https://opencontext.org/subjects/...")
                       or UUID

        Returns:
            SampleRecord if found, None otherwise
        """
        # Handle both full URIs and UUIDs
        if identifier.startswith("http"):
            url = identifier
            if not url.endswith(".json"):
                url = url.rstrip("/") + ".json"
        else:
            url = f"{self.base_url}/subjects/{identifier}.json"

        try:
            response = self.client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            return self._parse_single_record(data)
        except Exception as e:
            logger.warning(f"Failed to fetch OpenContext sample {identifier}: {e}")
            return None

    def get_samples_by_location(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 10.0,
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Find samples near a geographic point.

        OpenContext uses bounding box queries rather than radius,
        so we approximate with a square bounding box.

        Args:
            latitude: Center latitude in decimal degrees
            longitude: Center longitude in decimal degrees
            radius_km: Search radius in kilometers (converted to bbox)
            max_results: Maximum results to return

        Yields:
            SampleRecord objects within the search area
        """
        # Approximate degrees per km (varies by latitude)
        deg_per_km_lat = 1 / 111.0  # ~111 km per degree latitude
        deg_per_km_lon = 1 / (111.0 * abs(cos_deg(latitude)))

        # Create bounding box
        lat_delta = radius_km * deg_per_km_lat
        lon_delta = radius_km * deg_per_km_lon

        bbox = f"{longitude - lon_delta},{latitude - lat_delta},{longitude + lon_delta},{latitude + lat_delta}"

        url = f"{self.base_url}/query/.json"
        params = {
            **self.DEFAULT_PARAMS,
            "type": "subjects",
            "bbox": bbox,
            "rows": min(max_results, 100),
        }

        count = 0
        while url and count < max_results:
            response = self.client.get(url, params=params if count == 0 else None)
            response.raise_for_status()

            data = response.json()
            results = data.get("oc-api:has-results", [])

            if not results:
                break

            for record in results:
                if count >= max_results:
                    break
                sample = self._parse_record(record)
                if sample:
                    yield sample
                    count += 1

            url = data.get("next-json")
            params = None

    def list_projects(self, max_results: int = 100) -> Iterator[dict]:
        """
        List available OpenContext projects.

        Args:
            max_results: Maximum number of projects to return

        Yields:
            Project metadata dictionaries
        """
        url = f"{self.base_url}/query/.json"
        params = {"type": "projects", "rows": min(max_results, 100)}

        count = 0
        while url and count < max_results:
            response = self.client.get(url, params=params if count == 0 else None)
            response.raise_for_status()

            data = response.json()
            results = data.get("oc-api:has-results", [])

            for project in results:
                if count >= max_results:
                    break
                yield project
                count += 1

            url = data.get("next-json")
            params = None

    def _parse_record(self, record: dict) -> Optional[SampleRecord]:
        """
        Parse an OpenContext search result record into a SampleRecord.

        Args:
            record: Raw record from oc-api:has-results

        Returns:
            Parsed SampleRecord or None if parsing fails
        """
        try:
            identifier = record.get("citation uri") or record.get("uri", "")
            label = record.get("label", "Unknown")

            # Extract coordinates if available
            lat, lon = None, None
            if "latitude" in record and "longitude" in record:
                lat = float(record["latitude"])
                lon = float(record["longitude"])

            # Parse updated time
            collection_date = None
            if "updated" in record:
                collection_date = dateparser.parse(record["updated"])

            # Extract project/context info
            project = None
            context = record.get("context", "")
            if context:
                # Context is usually like "Project / Region / Site"
                parts = context.split("/")
                project = parts[0].strip() if parts else None

            return self._make_sample_record(
                identifier=identifier,
                label=label,
                raw_data=record,
                description=record.get("snippet"),
                latitude=lat,
                longitude=lon,
                material_type=record.get("category"),
                collection_date=collection_date,
                project=project,
            )
        except Exception as e:
            logger.warning(f"Failed to parse OpenContext record: {e}")
            return None

    def _parse_single_record(self, data: dict) -> Optional[SampleRecord]:
        """
        Parse a full single-record response into a SampleRecord.

        Args:
            data: Full JSON-LD response for a single record

        Returns:
            Parsed SampleRecord or None if parsing fails
        """
        try:
            identifier = data.get("id") or data.get("uri", "")
            label = data.get("label", data.get("dc-terms:title", "Unknown"))

            # Extract coordinates from features if available
            lat, lon = None, None
            features = data.get("features", [])
            if features:
                geom = features[0].get("geometry", {})
                if geom.get("type") == "Point":
                    coords = geom.get("coordinates", [])
                    if len(coords) >= 2:
                        lon, lat = coords[0], coords[1]

            return self._make_sample_record(
                identifier=identifier,
                label=label,
                raw_data=data,
                description=data.get("dc-terms:description"),
                latitude=lat,
                longitude=lon,
                material_type=data.get("category"),
                project=data.get("dc-terms:isPartOf", {}).get("label"),
            )
        except Exception as e:
            logger.warning(f"Failed to parse OpenContext single record: {e}")
            return None


def cos_deg(degrees: float) -> float:
    """Calculate cosine of angle in degrees."""
    import math
    return math.cos(math.radians(degrees))
