"""
Smithsonian Open Access API client for accessing museum collection data.

Smithsonian Open Access provides access to millions of digital items from
the Smithsonian Institution's collections.

API Documentation: https://www.si.edu/openaccess/devtools
API Key Registration: https://api.data.gov/signup/
"""

import logging
import os
from datetime import datetime
from typing import Any, Iterator, Optional

import dateparser

from .base import BaseSourceClient, SampleRecord

logger = logging.getLogger(__name__)


class SmithsonianClient(BaseSourceClient):
    """
    Client for the Smithsonian Open Access API.

    Smithsonian provides access to museum collections including
    natural history specimens, anthropological artifacts, and more.

    Example:
        >>> with SmithsonianClient(api_key="your_api_key") as si:
        ...     for sample in si.search("meteorite", max_results=10):
        ...         print(f"{sample.identifier}: {sample.label}")

    Note:
        Requires a free API key from api.data.gov.
        Set via api_key parameter or SMITHSONIAN_API_KEY environment variable.
    """

    SOURCE_NAME = "smithsonian"
    BASE_URL = "https://api.si.edu/openaccess/api/v1.0"

    # Categories relevant to physical samples
    SAMPLE_CATEGORIES = [
        "science_technology",  # Natural history, geology, paleontology
        "history_culture",     # Anthropology, archaeology
        "art_design",          # Objects with material composition
    ]

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
        user_agent: str = "isamples-client/1.0 (smithsonian)",
    ):
        """
        Initialize Smithsonian client.

        Args:
            api_key: Smithsonian API key (or set SMITHSONIAN_API_KEY env var)
            timeout: HTTP request timeout in seconds
            user_agent: User-Agent header value
        """
        super().__init__(
            base_url=self.BASE_URL,
            timeout=timeout,
            user_agent=user_agent,
        )
        self.api_key = api_key or os.environ.get("SMITHSONIAN_API_KEY")
        if not self.api_key:
            logger.warning(
                "No Smithsonian API key provided. "
                "Get a free key at https://api.data.gov/signup/"
            )

    def search(
        self,
        query: str,
        max_results: int = 100,
        category: Optional[str] = None,
        **kwargs: Any,
    ) -> Iterator[SampleRecord]:
        """
        Search Smithsonian collections for items matching a query.

        Args:
            query: Full-text search query
            max_results: Maximum number of results to return
            category: Optional category filter ('science_technology',
                      'history_culture', 'art_design')
            **kwargs: Additional query parameters

        Yields:
            SampleRecord objects matching the query
        """
        if not self.api_key:
            logger.error("API key required for Smithsonian searches")
            return

        # Use category-specific search if provided
        if category:
            url = f"{self.base_url}/category/{category}/search"
        else:
            url = f"{self.base_url}/search"

        start = 0
        rows = min(max_results, 1000)
        count = 0

        while count < max_results:
            params = {
                "api_key": self.api_key,
                "q": query,
                "start": start,
                "rows": rows,
                **kwargs,
            }

            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                response_data = data.get("response", {})
                rows_data = response_data.get("rows", [])

                if not rows_data:
                    break

                for record in rows_data:
                    if count >= max_results:
                        break
                    sample = self._parse_record(record)
                    if sample:
                        yield sample
                        count += 1

                # Check if there are more results
                total = response_data.get("rowCount", 0)
                start += len(rows_data)
                if start >= total or len(rows_data) < rows:
                    break

            except Exception as e:
                logger.warning(f"Smithsonian search failed: {e}")
                break

    def get_sample(self, identifier: str) -> Optional[SampleRecord]:
        """
        Fetch a single item by its Smithsonian ID.

        Args:
            identifier: Smithsonian item ID (e.g., "edanmdm-nmnh_paleobotany_...")

        Returns:
            SampleRecord if found, None otherwise
        """
        if not self.api_key:
            logger.error("API key required for Smithsonian queries")
            return None

        url = f"{self.base_url}/content/{identifier}"
        params = {"api_key": self.api_key}

        try:
            response = self.client.get(url, params=params)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            return self._parse_single_record(data)
        except Exception as e:
            logger.warning(f"Failed to fetch Smithsonian item {identifier}: {e}")
            return None

    def get_samples_by_location(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 10.0,
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Find items near a geographic point.

        Note: Smithsonian's API has limited geospatial search.
        This searches for location names/coordinates in metadata.

        Args:
            latitude: Center latitude in decimal degrees
            longitude: Center longitude in decimal degrees
            radius_km: Search radius (used for metadata search)
            max_results: Maximum results to return

        Yields:
            SampleRecord objects potentially near the location
        """
        # Smithsonian doesn't have native geo search
        # Search for coordinates in content
        query = f'"{latitude:.2f}" AND "{longitude:.2f}"'
        yield from self.search(query, max_results=max_results)

    def search_category(
        self,
        category: str,
        query: str = "",
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Search within a specific Smithsonian category.

        Args:
            category: Category name ('science_technology', 'history_culture', 'art_design')
            query: Optional search query within category
            max_results: Maximum results to return

        Yields:
            SampleRecord objects in the category
        """
        yield from self.search(query or "*", max_results=max_results, category=category)

    def list_terms(self, category: str = "object_type") -> Iterator[dict]:
        """
        List controlled vocabulary terms.

        Args:
            category: Term category ('object_type', 'place', 'topic', etc.)

        Yields:
            Term dictionaries
        """
        if not self.api_key:
            logger.error("API key required")
            return

        url = f"{self.base_url}/terms/{category}"
        params = {"api_key": self.api_key}

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            for term in data.get("response", {}).get("terms", []):
                yield term

        except Exception as e:
            logger.warning(f"Failed to list Smithsonian terms: {e}")

    def _parse_record(self, record: dict) -> Optional[SampleRecord]:
        """
        Parse a Smithsonian search result record.

        Args:
            record: Raw record from search results

        Returns:
            Parsed SampleRecord or None
        """
        try:
            identifier = record.get("id", "")
            content = record.get("content", {})
            descriptive = content.get("descriptiveNonRepeating", {})
            freetext = content.get("freetext", {})
            indexed = content.get("indexedStructured", {})

            # Get title/label
            label = (
                record.get("title", "")
                or descriptive.get("title", {}).get("content", "")
                or "Unknown"
            )

            # Get description
            description = None
            notes = freetext.get("notes", [])
            if notes and isinstance(notes, list):
                for note in notes:
                    if isinstance(note, dict):
                        description = note.get("content", "")
                        break

            # Extract material type
            material_type = None
            materials = indexed.get("material", [])
            if materials:
                material_type = materials[0] if isinstance(materials, list) else materials

            # Parse date
            collection_date = None
            dates = indexed.get("date", [])
            if dates:
                date_str = dates[0] if isinstance(dates, list) else dates
                collection_date = dateparser.parse(str(date_str))

            # Extract location (approximate)
            lat, lon = None, None
            geolocations = indexed.get("geoLocation", [])
            if geolocations:
                # GeoLocations may contain place names, not always coords
                pass  # Would need geocoding for place names

            return self._make_sample_record(
                identifier=identifier,
                label=label,
                raw_data=record,
                description=description,
                latitude=lat,
                longitude=lon,
                material_type=material_type,
                collection_date=collection_date,
                project=descriptive.get("data_source"),
            )
        except Exception as e:
            logger.warning(f"Failed to parse Smithsonian record: {e}")
            return None

    def _parse_single_record(self, data: dict) -> Optional[SampleRecord]:
        """
        Parse a single-item response.

        Args:
            data: Full item response

        Returns:
            Parsed SampleRecord or None
        """
        response = data.get("response", {})
        # Single item response wraps the record differently
        if response:
            return self._parse_record(response)
        return None
