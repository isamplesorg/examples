import logging
import typing
import urllib.parse
import httpx
import requests
import pandas as pd
import xarray

import pysolr
from datetime import datetime
from functools import partial

import multidict
import pysolr
from typing import List, Optional, Tuple, Union
from typing import Optional
import requests
import pandas as pd

ISB_SERVER = "https://central.isample.xyz/isamples_central/"
TIMEOUT = 10 #seconds
USER_AGENT = "Python/3.11 isamples.examples"

# in bytes, switch to POST if query string is longer than this value in the my_select method
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

# default field list to return in search results

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
    'f.producedBy_resultTimeRange.facet.range.end': '2024-01-01T00:00:00Z',
}

def format_date_for_solr(date_str):
    """
    Format the date string for Solr.

    Parameters:
        date_str (str): The date string to be formatted.

    Returns:
        str: The formatted date string in ISO 8601 format.

    Raises:
        ValueError: If the input date string is not in the expected format.

    """
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
        path = "%s/?%s" % (handler, params_encoded)
        return self._send_request("get", path)
    else:
        # Handles very long queries by submitting as a POST.
        path = "%s/" % handler
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
        TO DO: add support for additional parameters like fq, etc. or get rid of this method.
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



class IsbClient2(IsbClient):
    def __init__(self, url: str = 'https://central.isample.xyz/isamples_central/thing') -> None:
        """
        Initialize the IsbClient2 class.

        Args:
            url: The URL of the iSamples API.

        Returns:
            None.
        """
        super().__init__()
        self.url = url
        self.solr = pysolr.Solr(self.url, always_commit=True)

    def _fq_from_kwargs(self, collection_date_start: int = 1800, collection_date_end: str = 'NOW',
                        source: Optional[Tuple[str, ...]] = None, **kwargs) -> List[str]:
        """ 
        Build the filter query (fq) from a set of defaults and keyword arguments.

        Args:
            collection_date_start: The start date of the collection date range.
            collection_date_end: The end date of the collection date range.
            source: The source of the data.
            **kwargs: Additional filter conditions.

        Returns:
            List of filter query strings.
        """
        # build fq
        # 'field1': quote('value with spaces and special characters like &'),

        # source is a tuple drawing from ['SESAR', 'OPENCONTEXT', 'GEOME', 'SMITHSONIAN']
        if source is not None:
            source = " or ".join([f'"{s}"' for s in source])

        filter_conditions = multidict.MultiDict({
            'producedBy_resultTimeRange': f'[{collection_date_start} TO {collection_date_end}]',  # Range query
            'source': source,  # Boolean logic
            '-relation_target':'*'
        })

        # update filter_conditions with kwargs
        m = kwargs.get('_multi')
        if m is None:
            m = multidict.MultiDict(kwargs)
        else:
            del kwargs['_multi']
            m.extend(kwargs)
        filter_conditions.update(m)

        # Convert to list of fq strings
        fq = [f'{field}:{value}' for field, value in filter_null_values(filter_conditions).items()]

        # fq = ['producedBy_resultTimeRange:[1800 TO 2023]', 'source:(OPENCONTEXT or SESAR)', '-relation_target:*']
        return fq

    def default_search_params(self, q: str = '*:*',
                              fl: List[str] = FL_DEFAULT,
                              fq: Optional[List[str]] = None,
                              start: int = 0, rows: int = 20,
                              facet_field: List[str] = FACET_FIELDS_DEFAULT,
                              sort: str = 'id ASC',
                              **kwargs) -> dict:
        """
        Generate the default search parameters.

        Args:
            q: The query string.
            fl: The list of fields to return.
            fq: The filter query.
            start: The starting index of the search results.
            rows: The number of rows to return.
            facet_field: The fields to facet on.
            sort: The sort order.
            **kwargs: Additional parameters.

        Returns:
            Dictionary of search parameters.
        """
        if fq is None:
            fq = self._fq_from_kwargs()

        params = {
            'q': q,
            'fl': fl,
            'start': start,
            'rows': rows,
            'fq': fq,
            'facet': 'on',
            'facet.field': facet_field,
            'cursorMark': '*',
            'sort': sort,
        }

        # update params with kwargs
        params.update(kwargs)
        return params

    def search(self, params: Optional[dict] = None, **kwargs) -> Union[pysolr.Results, dict]:
        """
        Perform a search.

        Args:
            params: The search parameters.
            **kwargs: Additional parameters.

        Returns:
            Search results, which can be either a pysolr.Results object or a dictionary coming from thing/select.
        """
        if params is None:
            params = self.default_search_params(**kwargs)
        # give an option to pick how to do the search
        if kwargs.get('thingselect', False):
            return self._request("thing/select", params)
        else:
            return self.solr.search(**params)

    def record_count(self, params: Optional[dict] = None, **kwargs)->int:
        """
        Calculate the number of records matching the given search parameters.

        Args:
            params: The search parameters.
            **kwargs: Additional parameters.

        Returns:
            The number of records matching the search parameters.
        """

        response = self.search(params, **kwargs)

        if isinstance(response, pysolr.Results):
            return response.hits
        else:
            return response.get("response", {}).get("numFound", -1)

    def facets(self, params: Optional[dict] = None, **kwargs) -> typing.Dict[str, typing.Dict[str, int]]:
        """Get facet values and counts for the records based on the search parameters.
        Deduce the fields in question from params


        Response is a dict of dicts:
        {
            field_name: {
                facet_value: count,
                ...
            },
            ...
        }
        """
        params["rows"] = 0
        params["facet"] = "true"
        params["facet.mincount"] = 0

        # use the thing/select handler
        kwargs['thingselect'] = True
        response = self.search(params, **kwargs)

        res = {}
        for field in params.get("facet.field", []):
            counts = {}
            vals = response.get("facet_counts",{}).get("facet_fields",{}).get(field, [])
            for i in range(0, len(vals), 2):
                k = vals[i]
                v = vals[i+1]
                counts[k] = v
            res[field] = counts
        return res

    def pivot(self, params: dict, dimensions: typing.List[str], **kwargs)-> xarray.DataArray:
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

        params["rows"] = 0
        params["facet"] = "true"
        params["facet.mincount"] = 0
        params["facet.pivot"] = ",".join(dimensions)

        # use the thing/select handler
        kwargs['thingselect'] = True
        response = self.search(params, **kwargs)

        fkey = ",".join(dimensions)
        data = response.get("facet_counts", {}).get("facet_pivot", {}).get(fkey, [])
        coordinates = {k:[] for k in dimensions}
        _get_coordinates(data, dimensions, coordinates)
        values = _value_structure(dimensions, coordinates)
        xd = xarray.DataArray(values, coords=coordinates, dims=dimensions)
        _set_values(xd, data, {})
        return xd


class ISamplesBulkHandler:
    """
    A class for handling bulk operations in iSamples.

    Parameters:
    - token (str): The authentication token for accessing iSamples.
    - base_url (str, optional): The base URL for the iSamples API. Defaults to "https://central.isample.xyz/isamples_central/export".

    Methods:
    - create_download(query: str) -> str: Creates a download for the specified query.
    - get_status(uuid: str) -> dict: Retrieves the status of a download.
    - download_file(uuid: str, file_path: str) -> None: Downloads a file associated with the specified UUID.
    - load_dataset_to_dataframe(file_path: str) -> pd.DataFrame: Loads a dataset from a JSON file into a pandas DataFrame.
    """

    def __init__(self, token: str, base_url: str = "https://central.isample.xyz/isamples_central/export"):
        self.base_url = base_url
        self.token = token

    def create_download(self, query: str) -> str:
        """
        Creates a download for the specified query.

        Parameters:
        - query (str): The query for the download.

        Returns:
        - str: The UUID of the created download.

        Raises:
        - Exception: If the creation of the download fails.
        """
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {"q": query, "export_format": "jsonl"}
        response = requests.get(f"{self.base_url}/create", headers=headers, params=params)
        if response.status_code == 201:
            return response.json().get("uuid")
        else:
            raise Exception(f"Failed to create download: {response.text}")

    def get_status(self, uuid: str) -> dict:
        """
        Retrieves the status of a download.

        Parameters:
        - uuid (str): The UUID of the download.

        Returns:
        - dict: The status of the download.

        Raises:
        - Exception: If the retrieval of the status fails.
        """
        response = requests.get(f"{self.base_url}/status", params={"uuid": uuid})
        if response.status_code in (200, 202):
            return response.json()
        else:
            raise Exception(f"Failed to get status: {response.text}")

    def download_file(self, uuid: str, file_path: str) -> None:
        """
        Downloads a file associated with the specified UUID.

        Parameters:
        - uuid (str): The UUID of the file to download.
        - file_path (str): The path to save the downloaded file.

        Raises:
        - Exception: If the download fails.
        """
        response = requests.get(f"{self.base_url}/download", params={"uuid": uuid}, stream=True)
        print ("status code", response.status_code)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            raise Exception(f"Failed to download file: {response.text}")

    def load_dataset_to_dataframe(self, file_path: str) -> pd.DataFrame:
        """
        Loads a dataset from a JSON file into a pandas DataFrame.

        Parameters:
        - file_path (str): The path to the JSON file.

        Returns:
        - pandas.DataFrame: The loaded dataset.
        """
        return pd.read_json(file_path, lines=True)
