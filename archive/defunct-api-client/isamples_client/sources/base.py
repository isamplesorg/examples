"""
Base classes for iSamples source API clients.

Provides abstract interface and common data models for accessing
OpenContext, GEOME, SESAR, and Smithsonian sample repositories.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, Optional

import httpx


@dataclass
class SampleRecord:
    """
    Unified sample record for cross-source comparison.

    Provides a common interface regardless of which source
    (OpenContext, GEOME, SESAR, Smithsonian) the record came from.
    """

    source: str
    """Source identifier: 'opencontext', 'geome', 'sesar', 'smithsonian'"""

    identifier: str
    """Source-specific unique identifier (URI, IGSN, etc.)"""

    label: str
    """Human-readable title/name of the sample"""

    description: Optional[str] = None
    """Longer description if available"""

    latitude: Optional[float] = None
    """Geographic latitude in decimal degrees"""

    longitude: Optional[float] = None
    """Geographic longitude in decimal degrees"""

    material_type: Optional[str] = None
    """Material category (rock, tissue, artifact, etc.)"""

    collection_date: Optional[datetime] = None
    """When the sample was collected"""

    project: Optional[str] = None
    """Project or collection name"""

    raw_data: dict = field(default_factory=dict)
    """Original API response for this record"""

    @property
    def has_coordinates(self) -> bool:
        """Check if this sample has valid geographic coordinates."""
        return self.latitude is not None and self.longitude is not None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "source": self.source,
            "identifier": self.identifier,
            "label": self.label,
            "description": self.description,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "material_type": self.material_type,
            "collection_date": self.collection_date.isoformat() if self.collection_date else None,
            "project": self.project,
        }


class BaseSourceClient(ABC):
    """
    Abstract base class for all source API clients.

    Provides common HTTP client setup, context manager support,
    and defines the interface that all source clients must implement.

    Usage:
        with OpenContextClient() as client:
            for sample in client.search("pottery"):
                print(sample.label)
    """

    SOURCE_NAME: str = "unknown"
    """Override in subclasses with source identifier."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        user_agent: str = "isamples-client/1.0",
    ):
        """
        Initialize the source client.

        Args:
            base_url: Base URL for the API
            timeout: HTTP request timeout in seconds
            user_agent: User-Agent header value
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.user_agent = user_agent
        self._client: Optional[httpx.Client] = None

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialize and return HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                follow_redirects=True,
            )
        return self._client

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> "BaseSourceClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @abstractmethod
    def search(
        self,
        query: str,
        max_results: int = 100,
        **kwargs: Any,
    ) -> Iterator[SampleRecord]:
        """
        Search for samples matching a query string.

        Args:
            query: Search query text
            max_results: Maximum number of results to return
            **kwargs: Source-specific search parameters

        Yields:
            SampleRecord objects matching the query
        """
        pass

    @abstractmethod
    def get_sample(self, identifier: str) -> Optional[SampleRecord]:
        """
        Fetch a single sample by its identifier.

        Args:
            identifier: Source-specific sample identifier

        Returns:
            SampleRecord if found, None otherwise
        """
        pass

    @abstractmethod
    def get_samples_by_location(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 10.0,
        max_results: int = 100,
    ) -> Iterator[SampleRecord]:
        """
        Find samples near a geographic point.

        Args:
            latitude: Center point latitude in decimal degrees
            longitude: Center point longitude in decimal degrees
            radius_km: Search radius in kilometers
            max_results: Maximum number of results to return

        Yields:
            SampleRecord objects within the search radius
        """
        pass

    def _make_sample_record(
        self,
        identifier: str,
        label: str,
        raw_data: dict,
        **kwargs: Any,
    ) -> SampleRecord:
        """
        Helper to create a SampleRecord with common fields.

        Args:
            identifier: Sample identifier
            label: Sample label/title
            raw_data: Original API response
            **kwargs: Additional SampleRecord fields

        Returns:
            Populated SampleRecord
        """
        return SampleRecord(
            source=self.SOURCE_NAME,
            identifier=identifier,
            label=label,
            raw_data=raw_data,
            **kwargs,
        )
