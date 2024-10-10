import httpx
from urllib.parse import urlencode


params = {'q': '*:*',
 'fl': ('searchText',
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
  'hasSpecimenCategory'),
 'start': 0,
 'rows': 100,
 'fq': ['producedBy_resultTimeRange:[1800 TO NOW]',
  'source:"OPENCONTEXT"',
  '-relation_target:*'],
 'facet': 'on',
 'facet.field': ('authorizedBy',
  'hasContextCategory',
  'hasMaterialCategory',
  'registrant',
  'source',
  'hasSpecimenCategory'),
 'cursorMark': '*',
 'sort': 'id ASC',
 'facet.range': 'producedBy_resultTimeRange',
 'f.producedBy_resultTimeRange.facet.range.gap': '+1YEARS',
 'f.producedBy_resultTimeRange.facet.range.start': '1800-01-01T00:00:00Z',
 'f.producedBy_resultTimeRange.facet.range.end': '2023-01-01T00:00:00Z'}

ISB_SERVER = "https://central.isample.xyz/isamples_central/"

# get
r = httpx.request('GET', f'{ISB_SERVER}/thing/select', params=params)
print('GET')
print(r.json()['responseHeader']['params'])

# post
headers = {
    "Content-type": "application/x-www-form-urlencoded; charset=utf-8"
}

params_encoded = urlencode(params)
r1 = httpx.post(f'{ISB_SERVER}/thing/select', data=params_encoded, headers=headers)
print('POST')
print(r1.json()['responseHeader']['params'])
