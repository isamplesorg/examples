import json
import logging
import typing
import urllib.parse
import httpx
import xarray

ISB_SERVER = "https://central.isample.xyz/isamples_central/"
TIMEOUT = 10 #seconds
USER_AGENT = "Python/3.11 isamples.examples"

logging.basicConfig(level=logging.INFO)
L = logging.getLogger()


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

