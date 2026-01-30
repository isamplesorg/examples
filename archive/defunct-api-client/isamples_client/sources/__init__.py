"""
iSamples Source API Clients.

Provides unified Python clients for accessing sample data from:
- OpenContext (archaeological samples)
- SESAR (geological samples via IGSN)
- GEOME (genomic/biological samples)
- Smithsonian (museum collections)

Example:
    >>> from isamples_client.sources import OpenContextClient, SESARClient
    >>>
    >>> # Search OpenContext for pottery samples
    >>> with OpenContextClient() as oc:
    ...     for sample in oc.search("pottery", max_results=5):
    ...         print(f"{sample.source}: {sample.label}")
    ...
    >>> # Fetch a SESAR sample by IGSN
    >>> with SESARClient() as sesar:
    ...     sample = sesar.get_sample("IEWFS0001")
    ...     if sample:
    ...         print(f"{sample.identifier}: {sample.material_type}")
"""

from .base import BaseSourceClient, SampleRecord
from .geome import GEOMEClient
from .opencontext import OpenContextClient
from .sesar import SESARClient
from .smithsonian import SmithsonianClient

__all__ = [
    # Base classes
    "BaseSourceClient",
    "SampleRecord",
    # Source clients
    "OpenContextClient",
    "SESARClient",
    "GEOMEClient",
    "SmithsonianClient",
]
