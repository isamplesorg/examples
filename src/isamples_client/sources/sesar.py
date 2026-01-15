"""
SESAR API client for accessing geological sample data.

SESAR (System for Earth Sample Registration) provides access to geological
samples identified by IGSNs (International Geo Sample Numbers).

API Documentation: https://api.geosamples.org/swagger-ui.html
"""

import logging
from datetime import datetime
from typing import Any, Iterator, Optional

import dateparser

from .base import BaseSourceClient, SampleRecord

logger = logging.getLogger(__name__)


class SESARClient(BaseSourceClient):
    """
    Client for the SESAR geological sample API.

    SESAR provides geological sample data including rock cores,
    minerals, sediments, and other earth science specimens.
    Samples are identified by IGSNs.

    Example:
        >>> with SESARClient() as sesar:
        ...     sample = sesar.get_sample("IEWFS0001")
        ...     if sample:
        ...         print(f"{sample.identifier}: {sample.label}")

    Note:
        Read operations are open. Write operations require a MySESAR account.
    """

    SOURCE_NAME = "sesar"
    BASE_URL = "https://api.geosamples.org"
    APP_URL = "https://app.geosamples.org"

    def __init__(
        self,
        timeout: float = 30.0,
        user_agent: str = "isamples-client/1.0 (sesar)",
    ):
        """
        Initialize SESAR client.

        Args:
            timeout: HTTP request timeout in seconds
            user_agent: User-Agent header value
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
        **kwargs: Any,
    ) -> Iterator[SampleRecord]:
        """
        Search SESAR for samples.

        Note: SESAR's search API is limited. For comprehensive searches,
        consider using get_samples_by_location or iterating IGSNs.

        Args:
            query: Search query (sample name, material type, etc.)
            max_results: Maximum number of results to return
            **kwargs: Additional query parameters

        Yields:
            SampleRecord objects matching the query
        """
        # SESAR doesn't have a great full-text search API
        # Use the samples endpoint with filters
        url = f"{self.base_url}/v1/samples"
        params = {
            "limit": min(max_results, 100),
            **kwargs,
        }

        # If query looks like an IGSN, fetch directly
        if self._looks_like_igsn(query):
            sample = self.get_sample(query)
            if sample:
                yield sample
            return

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            count = 0
            for record in data.get("samples", data if isinstance(data, list) else []):
                if count >= max_results:
                    break
                sample = self._parse_record(record)
                if sample:
                    yield sample
                    count += 1

        except Exception as e:
            logger.warning(f"SESAR search failed: {e}")

    def get_sample(self, igsn: str) -> Optional[SampleRecord]:
        """
        Fetch a single sample by its IGSN.

        Args:
            igsn: International Geo Sample Number (e.g., "IEWFS0001")

        Returns:
            SampleRecord if found, None otherwise
        """
        # Normalize IGSN
        igsn_value = self._normalize_igsn(igsn)

        # Try JSON-LD endpoint first (preferred)
        url = f"{self.base_url}/v1/sample/igsn-ev-json-ld/igsn/{igsn_value}"
        headers = {"Accept": "application/ld+json, application/json"}

        try:
            response = self.client.get(url, headers=headers)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            return self._parse_jsonld_record(data, igsn_value)
        except Exception as e:
            logger.warning(f"Failed to fetch SESAR sample {igsn}: {e}")
            # Fall back to app.geosamples.org
            return self._get_sample_app(igsn_value)

    def _get_sample_app(self, igsn_value: str) -> Optional[SampleRecord]:
        """
        Fallback: fetch sample from legacy app endpoint.

        Args:
            igsn_value: IGSN value

        Returns:
            SampleRecord if found, None otherwise
        """
        url = f"{self.APP_URL}/webservices/display.php"
        params = {"igsn": igsn_value}

        try:
            response = self.client.get(url, params=params)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            return self._parse_app_record(data, igsn_value)
        except Exception as e:
            logger.warning(f"SESAR app fallback failed for {igsn_value}: {e}")
            return None

    def get_samples_by_location(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 10.0,
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Find samples near a geographic point using polygon query.

        SESAR supports polygon-based geospatial queries.

        Args:
            latitude: Center latitude in decimal degrees
            longitude: Center longitude in decimal degrees
            radius_km: Search radius in kilometers
            max_results: Maximum results to return

        Yields:
            SampleRecord objects within the search area
        """
        # Create bounding box polygon
        deg_per_km_lat = 1 / 111.0
        import math
        deg_per_km_lon = 1 / (111.0 * abs(math.cos(math.radians(latitude))))

        lat_delta = radius_km * deg_per_km_lat
        lon_delta = radius_km * deg_per_km_lon

        # Create polygon coordinates (WKT format for some endpoints)
        min_lat = latitude - lat_delta
        max_lat = latitude + lat_delta
        min_lon = longitude - lon_delta
        max_lon = longitude + lon_delta

        # Use bounding box parameters
        url = f"{self.base_url}/v1/samples"
        params = {
            "north": max_lat,
            "south": min_lat,
            "east": max_lon,
            "west": min_lon,
            "limit": min(max_results, 100),
        }

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            count = 0
            samples = data.get("samples", data if isinstance(data, list) else [])
            for record in samples:
                if count >= max_results:
                    break
                sample = self._parse_record(record)
                if sample:
                    yield sample
                    count += 1

        except Exception as e:
            logger.warning(f"SESAR geospatial query failed: {e}")

    def get_sample_by_user(
        self,
        user_code: str,
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Get samples registered by a specific user.

        Args:
            user_code: SESAR user code
            max_results: Maximum results to return

        Yields:
            SampleRecord objects registered by the user
        """
        url = f"{self.APP_URL}/webservices/igsn_list.php"
        params = {"user_code": user_code}

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            igsns = data.get("igsns", [])
            count = 0
            for igsn in igsns[:max_results]:
                sample = self.get_sample(igsn)
                if sample:
                    yield sample
                    count += 1

        except Exception as e:
            logger.warning(f"SESAR user query failed: {e}")

    def _parse_jsonld_record(
        self, data: dict, igsn: str
    ) -> Optional[SampleRecord]:
        """
        Parse JSON-LD format SESAR record.

        Args:
            data: JSON-LD response data
            igsn: IGSN identifier

        Returns:
            Parsed SampleRecord or None
        """
        try:
            desc = data.get("description", {})

            label = desc.get("sampleName", f"IGSN:{igsn}")
            sample_type = desc.get("sampleType")
            material = desc.get("material")

            # Extract coordinates
            lat, lon = None, None
            if "latitude" in desc:
                lat = float(desc["latitude"])
            if "longitude" in desc:
                lon = float(desc["longitude"])

            # Parse collection date
            collection_date = None
            date_str = desc.get("collectionStartDate")
            if date_str:
                collection_date = dateparser.parse(date_str)

            return self._make_sample_record(
                identifier=f"igsn:{igsn}",
                label=label,
                raw_data=data,
                description=desc.get("description"),
                latitude=lat,
                longitude=lon,
                material_type=material or sample_type,
                collection_date=collection_date,
                project=desc.get("collectionMethod"),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SESAR JSON-LD record: {e}")
            return None

    def _parse_app_record(
        self, data: dict, igsn: str
    ) -> Optional[SampleRecord]:
        """
        Parse legacy app format SESAR record.

        Args:
            data: App endpoint response data
            igsn: IGSN identifier

        Returns:
            Parsed SampleRecord or None
        """
        try:
            sample = data.get("sample", data)

            label = sample.get("name", f"IGSN:{igsn}")
            sample_type = sample.get("sample_type")
            material = sample.get("material")

            lat = sample.get("latitude")
            lon = sample.get("longitude")
            if lat is not None:
                lat = float(lat)
            if lon is not None:
                lon = float(lon)

            return self._make_sample_record(
                identifier=f"igsn:{igsn}",
                label=label,
                raw_data=data,
                description=sample.get("description"),
                latitude=lat,
                longitude=lon,
                material_type=material or sample_type,
            )
        except Exception as e:
            logger.warning(f"Failed to parse SESAR app record: {e}")
            return None

    def _parse_record(self, record: dict) -> Optional[SampleRecord]:
        """
        Parse a generic SESAR record.

        Args:
            record: Record from search results

        Returns:
            Parsed SampleRecord or None
        """
        try:
            igsn = record.get("igsn", "")
            label = record.get("name", record.get("sampleName", f"IGSN:{igsn}"))

            lat = record.get("latitude")
            lon = record.get("longitude")
            if lat is not None:
                lat = float(lat)
            if lon is not None:
                lon = float(lon)

            return self._make_sample_record(
                identifier=f"igsn:{igsn}" if igsn else record.get("id", ""),
                label=label,
                raw_data=record,
                description=record.get("description"),
                latitude=lat,
                longitude=lon,
                material_type=record.get("material") or record.get("sample_type"),
            )
        except Exception as e:
            logger.warning(f"Failed to parse SESAR record: {e}")
            return None

    def _normalize_igsn(self, igsn: str) -> str:
        """
        Normalize IGSN to value-only format.

        Args:
            igsn: IGSN in various formats

        Returns:
            Normalized IGSN value
        """
        # Remove common prefixes
        igsn = igsn.strip()
        for prefix in ["igsn:", "IGSN:", "https://igsn.org/", "http://igsn.org/"]:
            if igsn.startswith(prefix):
                igsn = igsn[len(prefix):]
        return igsn.upper()

    def _looks_like_igsn(self, query: str) -> bool:
        """Check if query looks like an IGSN."""
        q = query.strip().upper()
        # IGSNs are typically 9 alphanumeric characters
        if len(q) >= 5 and q.isalnum():
            return True
        if q.startswith("IGSN:"):
            return True
        return False
