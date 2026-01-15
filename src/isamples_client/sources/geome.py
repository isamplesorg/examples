"""
GEOME API client for accessing genomic/biological sample data.

GEOME (Genomic Observatories Metadatabase) provides access to biological
samples with genetic/genomic metadata.

API Documentation: https://api.geome-db.org/apidocs/
"""

import logging
from datetime import datetime
from typing import Any, Iterator, Optional

import dateparser

from .base import BaseSourceClient, SampleRecord

logger = logging.getLogger(__name__)


class GEOMEClient(BaseSourceClient):
    """
    Client for the GEOME genomic sample API.

    GEOME provides biological sample data including tissue samples,
    DNA extracts, and environmental samples with links to genetic
    sequences in NCBI/GenBank.

    The API is organized hierarchically:
    - Networks contain Projects
    - Projects contain Expeditions
    - Expeditions contain Samples

    Example:
        >>> with GEOMEClient() as geome:
        ...     for project in geome.list_projects(max_results=5):
        ...         print(project['projectTitle'])

    Note:
        Read operations are open. Write operations require OAuth 2.0.
    """

    SOURCE_NAME = "geome"
    BASE_URL = "https://api.geome-db.org/v1"

    def __init__(
        self,
        timeout: float = 30.0,
        user_agent: str = "isamples-client/1.0 (geome)",
    ):
        """
        Initialize GEOME client.

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
        entity: str = "Sample",
        **kwargs: Any,
    ) -> Iterator[SampleRecord]:
        """
        Search GEOME for samples matching a query.

        Args:
            query: Search query (uses GEOME query syntax)
            max_results: Maximum number of results to return
            entity: Entity type to search ('Sample', 'Tissue', 'Event', etc.)
            **kwargs: Additional query parameters

        Yields:
            SampleRecord objects matching the query
        """
        url = f"{self.base_url}/records/{entity}/json"
        page = 0
        page_size = min(max_results, 1000)
        count = 0

        while count < max_results:
            params = {
                "limit": page_size,
                "page": page,
                "q": query,
                **kwargs,
            }

            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                content = data.get("content", {})
                records = content.get(entity, [])

                if not records:
                    break

                for record in records:
                    if count >= max_results:
                        break
                    sample = self._parse_record(record, entity)
                    if sample:
                        yield sample
                        count += 1

                # Check if there are more pages
                total = data.get("totalElements", 0)
                if count >= total or len(records) < page_size:
                    break

                page += 1

            except Exception as e:
                logger.warning(f"GEOME search failed: {e}")
                break

    def get_sample(self, identifier: str) -> Optional[SampleRecord]:
        """
        Fetch a single sample by its BCID (GEOME identifier).

        Args:
            identifier: GEOME BCID identifier

        Returns:
            SampleRecord if found, None otherwise
        """
        # Normalize identifier
        bcid = identifier.strip()

        # GEOME uses BCIDs which may have ark: prefix
        url = f"{self.base_url}/records/{bcid}"

        try:
            response = self.client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            return self._parse_single_record(data)
        except Exception as e:
            logger.warning(f"Failed to fetch GEOME sample {identifier}: {e}")
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

        Note: GEOME stores coordinates in Event entities, not Sample entities.
        This method queries Events and returns them as SampleRecords.

        Args:
            latitude: Center latitude in decimal degrees
            longitude: Center longitude in decimal degrees
            radius_km: Search radius in kilometers
            max_results: Maximum results to return

        Yields:
            SampleRecord objects within the search area
        """
        # Create bounding box query
        import math
        deg_per_km_lat = 1 / 111.0
        deg_per_km_lon = 1 / (111.0 * abs(math.cos(math.radians(latitude))))

        lat_delta = radius_km * deg_per_km_lat
        lon_delta = radius_km * deg_per_km_lon

        min_lat = latitude - lat_delta
        max_lat = latitude + lat_delta
        min_lon = longitude - lon_delta
        max_lon = longitude + lon_delta

        # GEOME uses Lucene range syntax [min TO max]
        # Coordinates are stored in Event entity, not Sample
        query = (
            f"decimalLatitude:[{min_lat} TO {max_lat}] "
            f"AND decimalLongitude:[{min_lon} TO {max_lon}]"
        )

        # Query Event entity since that's where coordinates are stored
        yield from self.search(query, max_results=max_results, entity="Event")

    def list_projects(self, max_results: int = 100) -> Iterator[dict]:
        """
        List available GEOME projects.

        Args:
            max_results: Maximum number of projects to return

        Yields:
            Project metadata dictionaries
        """
        url = f"{self.base_url}/projects"
        params = {"includePublic": "true", "admin": "false"}

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            projects = response.json()

            for i, project in enumerate(projects):
                if i >= max_results:
                    break
                yield project

        except Exception as e:
            logger.warning(f"Failed to list GEOME projects: {e}")

    def list_expeditions(
        self,
        project_id: int,
        max_results: int = 100,
    ) -> Iterator[dict]:
        """
        List expeditions within a project.

        Args:
            project_id: GEOME project ID
            max_results: Maximum number of expeditions to return

        Yields:
            Expedition metadata dictionaries
        """
        url = f"{self.base_url}/projects/{project_id}/expeditions"
        params = {"includePublic": "true", "admin": "false"}

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            expeditions = response.json()

            for i, expedition in enumerate(expeditions):
                if i >= max_results:
                    break
                if isinstance(expedition, dict):
                    yield expedition

        except Exception as e:
            logger.warning(f"Failed to list expeditions for project {project_id}: {e}")

    def get_samples_in_expedition(
        self,
        expedition_code: str,
        entity: str = "Sample",
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Get all samples in a specific expedition.

        Args:
            expedition_code: Expedition code
            entity: Entity type ('Sample', 'Tissue', 'Event')
            max_results: Maximum results to return

        Yields:
            SampleRecord objects in the expedition
        """
        query = f"_expeditions_:{expedition_code}"
        yield from self.search(query, max_results=max_results, entity=entity)

    def export_to_csv(
        self,
        query: str,
        entity: str = "Sample",
    ) -> bytes:
        """
        Export query results as CSV.

        Args:
            query: GEOME query string
            entity: Entity type

        Returns:
            CSV data as bytes
        """
        url = f"{self.base_url}/records/{entity}/csv"
        params = {"q": query}

        response = self.client.get(url, params=params)
        response.raise_for_status()
        return response.content

    def _parse_record(
        self, record: dict, entity: str = "Sample"
    ) -> Optional[SampleRecord]:
        """
        Parse a GEOME record into a SampleRecord.

        Args:
            record: Raw GEOME record
            entity: Entity type ('Sample', 'Event', 'Tissue', etc.)

        Returns:
            Parsed SampleRecord or None
        """
        try:
            identifier = record.get("bcid", record.get("materialSampleID", record.get("eventID", "")))

            # Label depends on entity type
            if entity == "Event":
                label = record.get("locality", record.get("eventID", identifier))
            else:
                label = record.get("materialSampleID", record.get("sampleID", identifier))

            # Extract coordinates
            lat = record.get("decimalLatitude")
            lon = record.get("decimalLongitude")
            if lat is not None:
                lat = float(lat)
            if lon is not None:
                lon = float(lon)

            # Parse collection date from year/month/day fields
            collection_date = self._parse_date_fields(record)

            # Extract material/tissue type
            material_type = (
                record.get("tissueType")
                or record.get("preparationType")
                or record.get("sampleType")
            )

            return self._make_sample_record(
                identifier=identifier,
                label=label,
                raw_data=record,
                description=record.get("locality"),
                latitude=lat,
                longitude=lon,
                material_type=material_type,
                collection_date=collection_date,
                project=record.get("_projects_", record.get("projectId")),
            )
        except Exception as e:
            logger.warning(f"Failed to parse GEOME record: {e}")
            return None

    def _parse_single_record(self, data: dict) -> Optional[SampleRecord]:
        """
        Parse a single-record response.

        Args:
            data: Full record response

        Returns:
            Parsed SampleRecord or None
        """
        # Single record responses may have different structure
        if "content" in data:
            # Response contains content wrapper
            content = data.get("content", {})
            for entity_type in ["Sample", "Tissue", "Event"]:
                records = content.get(entity_type, [])
                if records:
                    return self._parse_record(records[0], entity_type)
        else:
            # Direct record
            return self._parse_record(data)
        return None

    def _parse_date_fields(self, record: dict) -> Optional[datetime]:
        """
        Parse date from GEOME's year/month/day fields.

        Args:
            record: Record with date fields

        Returns:
            Parsed datetime or None
        """
        try:
            year = record.get("yearCollected", "")
            if not year:
                return None

            date_str = str(year)
            month = record.get("monthCollected", "")
            if month:
                date_str = f"{year}-{month:02d}" if isinstance(month, int) else f"{year}-{month}"
                day = record.get("dayCollected", "")
                if day:
                    date_str = f"{date_str}-{day:02d}" if isinstance(day, int) else f"{date_str}-{day}"
                    time_of_day = record.get("timeOfDay", "")
                    if time_of_day:
                        date_str = f"{date_str} {time_of_day}"

            return dateparser.parse(date_str)
        except Exception:
            return None
