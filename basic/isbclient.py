import json
import logging
import typing
import urllib.parse
import httpx
import xarray

import pysolr
from datetime import datetime
from functools import partial

ISB_SERVER = "https://central.isample.xyz/isamples_central/"
TIMEOUT = 10 #seconds
USER_AGENT = "Python/3.11 isamples.examples"

SWITCH_TO_POST=10000

# fields used in https://central.isample.xyz/isamples_central/ui

MAJOR_FIELDS = dict([('All text fields', 'searchText'),
 ('Collection Date', 'producedBy_resultTimeRange'),
 ('Context', 'hasContextCategory'),
 ('Identifier', 'id'),
 ('Keywords', 'keywords'),
 ('Label', 'label'),
 ('Material', 'hasMaterialCategory'),
 ('ProducedBy ResultTime',  'producedBy_resultTime'),
 ('ProducedBy SamplingSite PlaceName', 'producedBy_samplingSite_placeName'),
 ('Registrant', 'registrant'),
 ('Source', 'source'),
 ('Source Updated Time', 'sourceUpdatedTime'),
 ('Spatial Query', 'producedBy_samplingSite_location_rpt'),
 ('Specimen', 'hasSpecimenCategory')])

# default field list to return

FL_DEFAULT = ('searchText',
 'authorizedBy',
 'producedBy_resultTimeRange',
 'hasContextCategory',
 'curation_accessContraints',
 'curation_description_text',
 'curation_label',
 'curation_location',
 'curation_responsibility',
 'description_text',
 'id',
 'informalClassification',
 'keywords',
 'label',
 'hasMaterialCategory',
 'producedBy_description_text',
 'producedBy_hasFeatureOfInterest',
 'producedBy_label',
 'producedBy_responsibility',
 'producedBy_resultTime',
 'producedBy_samplingSite_description_text',
 'producedBy_samplingSite_label',
 'producedBy_samplingSite_location_elevationInMeters',
 'producedBy_samplingSite_location_latitude',
 'producedBy_samplingSite_location_longitude',
 'producedBy_samplingSite_placeName',
 'registrant',
 'samplingPurpose',
 'source',
 'sourceUpdatedTime',
 'producedBy_samplingSite_location_rpt',
 'hasSpecimenCategory')

FACET_FIELDS_DEFAULT = ('authorizedBy', 'hasContextCategory', 'hasMaterialCategory', 'registrant', 'source', 'hasSpecimenCategory')

# https://solr.apache.org/guide/8_11/faceting.html#range-faceting

FACET_RANGE_FIELDS_DEFAULT = {
    'facet.range': 'producedBy_resultTimeRange',
    'f.producedBy_resultTimeRange.facet.range.gap': '+1YEARS',
    'f.producedBy_resultTimeRange.facet.range.start': '1800-01-01T00:00:00Z',
    'f.producedBy_resultTimeRange.facet.range.end': '2023-01-01T00:00:00Z',
}

def format_date_for_solr(date_str):
    # Assuming the input is in a format like 'YYYY-MM-DD' or already in ISO 8601
    # Modify this part if your input format is different
    try:
        # If the date is already in ISO 8601 format, return as is
        datetime.fromisoformat(date_str)
        return date_str
    except ValueError:
        # Convert from 'YYYY-MM-DD' to ISO 8601
        return datetime.strptime(date_str, '%Y-%m-%d').isoformat() + 'Z'

def create_date_range_query(start_str, end_str):
    # If start_str or end_str is blank, use '*' for open-ended range
    start_date = format_date_for_solr(start_str) if start_str else '*'
    end_date = format_date_for_solr(end_str) if end_str else '*'
    return f'[{start_date} TO {end_date}]'

def filter_null_values(d):
    return {k:v for k,v in d.items() if v is not None}

ISAMPLES_SOURCES = ['SESAR',
    'OPENCONTEXT',
    'GEOME',
    'SMITHSONIAN',
]

logging.basicConfig(level=logging.INFO)
L = logging.getLogger()

def my_select(self, params, handler=None):
    """
    :param params:
    :param handler: defaults to self.search_handler (fallback to 'select')
    :return:
    """
    # Returns json docs unless otherwise specified
    params.setdefault("wt", "json")
    custom_handler = handler or self.search_handler
    handler = "select"
    if custom_handler:
        if self.use_qt_param:
            params["qt"] = custom_handler
        else:
            handler = custom_handler

    params_encoded = pysolr.safe_urlencode(params, True)

    # put no effective limit on the size of the query
    if len(params_encoded) < SWITCH_TO_POST:
        # Typical case.
        path = "%s?%s" % (handler, params_encoded)
        return self._send_request("get", path)
    else:
        # Handles very long queries by submitting as a POST.
        path = "%s" % handler
        headers = {
            "Content-type": "application/x-www-form-urlencoded; charset=utf-8"
        }
        return self._send_request(
            "post", path, body=params_encoded, headers=headers
        )

# cache the original select method
pysolr.Solr._select_orig = pysolr.Solr._select


def monkey_patch_select(active=False):
    """
    :param active: if True, monkey patch pysolr.Solr._select
    :param switch_to_post: if the query string is longer than this, switch to POST
    :return:
    """
    if active:
        pysolr.Solr._select = my_select
    else:
        pysolr.Solr._select = pysolr.Solr._select_orig


class IsbClient:
    """A client for iSamples.
    """

    def __init__(self, isb_server:str=None):
        self.isb_server = ISB_SERVER if isb_server is None else isb_server
        self.isb_server = self.isb_server.strip(" /") + "/"
        self.session = httpx.Client()

    def _request(self, path:str, params=None)->typing.Any:
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT
        }
        url = urllib.parse.urljoin(self.isb_server, path)
        response = self.session.get(url, params=params, headers=headers, timeout=TIMEOUT)
        L.info("url = %s", response.url)
        return response.json()

    def field_names(self)->typing.List[str]:
        """Return a list of field names available in the Solr endpoint.
        """
        response = self._request("thing/select/info")
        fields = [k for k in response.get("schema",{}).get("fields", {}).keys()]
        return fields

    def record_count(self, q:str)->int:
        """Number of records matching query q
        """
        params = httpx.QueryParams(rows=0, q=q)
        response = self._request("thing/select", params)
        return response.get("response", {}).get("numFound", -1)

    def facets(self, q:str, fields:typing.List[str]) -> typing.Dict[str, typing.Dict[str, int]]:
        """Get facet values and counts for the records matching query q and specified fields.

        Response is a dict of dicts:
        {
            field_name: {
                facet_value: count,
                ...
            },
            ...
        }
        """
        params = httpx.QueryParams(rows=0, q=q, facet="true")
        params = params.add("facet.mincount", 0)
        for field in fields:
            params = params.add("facet.field", field)
        response = self._request("thing/select", params)
        res = {}
        for field in fields:
            counts = {}
            vals = response.get("facet_counts",{}).get("facet_fields",{}).get(field, [])
            for i in range(0, len(vals), 2):
                k = vals[i]
                v = vals[i+1]
                counts[k] = v
            res[field] = counts
        return res


    def pivot(self, q:str, dimensions:typing.List[str])-> xarray.DataArray:
        """Return an n-dimensional xarray of counts for specified fields
        """

        def _normalize_facet(v:str):
            return v.strip().lower()

        def _get_coordinates(data, dimensions, coordinates):
            """Get the coordinate index values from the facet response.            
            """
            for entry in data:
                v = _normalize_facet(entry.get("value"))
                f = entry.get("field")
                if f is not None and v not in coordinates[f]:
                    coordinates[f].append(v)
                _get_coordinates(entry.get("pivot", []), dimensions, coordinates)

        def _value_structure(dimensions, coordinates, cdim=0):
            """Populate an empty value structure for holding the facet counts
            """
            nvalues = len(coordinates[dimensions[cdim]])
            if cdim >= len(dimensions)-1:
                return [0,]*nvalues
            return [_value_structure(dimensions, coordinates, cdim=cdim+1)]*nvalues

        def _set_values(values, data, coord):
            """Populate the xarray with the facet count values.
            """
            for entry in data:
                coord[entry.get("field")] = _normalize_facet(entry.get("value"))
                p = entry.get("pivot", None)
                if p is None:
                    values.loc[coord] = values.loc[coord]  + entry.get("count")
                else:
                    _set_values(values, p, coord)
                coord.popitem()

        if len(dimensions) < 2:
            raise ValueError("At least two dimensions required for pivot.")
        params = httpx.QueryParams(rows=0, q=q)
        params = params.add("facet", "true")
        params = params.add("facet.mincount", 0)
        params = params.add("facet.pivot", ",".join(dimensions))
        response = self._request("thing/select", params)
        fkey = ",".join(dimensions)
        data = response.get("facet_counts", {}).get("facet_pivot", {}).get(fkey, [])
        coordinates = {k:[] for k in dimensions}
        _get_coordinates(data, dimensions, coordinates)
        values = _value_structure(dimensions, coordinates)
        xd = xarray.DataArray(values, coords=coordinates, dims=dimensions)
        _set_values(xd, data, {})
        return xd

