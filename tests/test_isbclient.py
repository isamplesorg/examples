"""Tests for the isbclient module."""

# pylint: disable=no-name-in-module
from isamples_client import IsbClient2


def test_field_names():
    """Test the field_names method."""
    client = IsbClient2()
    fields = client.field_names()
    assert isinstance(fields, list)
    assert len(fields) > 0
    assert all(isinstance(field, str) for field in fields)
