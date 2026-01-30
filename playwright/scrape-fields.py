import asyncio
from playwright.async_api import async_playwright

from datetime import datetime

MIN_YEAR = 1800
MAX_YEAR = datetime.now().year

# Define your fields array
fields = [
    {"field": "curation_accessContraints", "type": "non-search", "hidden": True},
    {"field": "curation_description_text", "type": "non-search", "hidden": True},
    {"field": "curation_label", "type": "non-search", "hidden": True},
    {"field": "curation_location", "type": "non-search", "hidden": True},
    {"field": "curation_responsibility", "type": "non-search", "hidden": True},
    {"field": "description_text", "type": "non-search", "hidden": True},
    {
        "label": "Context",
        "field": "hasContextCategory",
        "type": "hierarchy-facet",
        "collapse": True,
    },
    {
        "label": "Material",
        "field": "hasMaterialCategory",
        "type": "hierarchy-facet",
        "collapse": True,
    },
    {
        "label": "Specimen",
        "field": "hasSpecimenCategory",
        "type": "hierarchy-facet",
        "collapse": True,
    },
    {"label": "Identifier", "field": "id", "type": "text"},
    {"field": "informalClassification", "type": "non-search", "hidden": True},
    {"field": "keywords", "type": "text"},
    {"field": "label", "type": "non-search"},
    {"field": "producedBy_description_text", "type": "non-search", "hidden": True},
    {"field": "producedBy_hasFeatureOfInterest", "type": "non-search", "hidden": True},
    {"field": "producedBy_label", "type": "non-search", "hidden": True},
    {"field": "producedBy_responsibility", "type": "non-search", "hidden": True},
    {"field": "producedBy_resultTime", "type": "non-search"},
    {
        "label": "Collection Date",
        "field": "producedBy_resultTimeRange",
        "type": "date-range-facet",
        "minValue": "MIN_YEAR",
        "maxValue": "MAX_YEAR",
        "value": ["MIN_YEAR", "MAX_YEAR"],
    },
    {
        "field": "producedBy_samplingSite_description_text",
        "type": "non-search",
        "hidden": True,
    },
    {"field": "producedBy_samplingSite_label", "type": "non-search", "hidden": True},
    {
        "field": "producedBy_samplingSite_location_elevationInMeters",
        "type": "non-search",
        "hidden": True,
    },
    {
        "field": "producedBy_samplingSite_location_latitude",
        "type": "non-search",
        "hidden": True,
    },
    {
        "field": "producedBy_samplingSite_location_longitude",
        "type": "non-search",
        "hidden": True,
    },
    {"field": "producedBy_samplingSite_placeName", "type": "non-search"},
    {
        "field": "registrant",
        "type": "list-facet",
        "facetSort": "count",
        "collapse": True,
    },
    {"field": "samplingPurpose", "type": "non-search", "hidden": True},
    {"label": "All text fields", "field": "searchText", "type": "text"},
    {"field": "source", "type": "list-facet", "facetSort": "index", "collapse": True},
    {"field": "sourceUpdatedTime", "type": "non-search", "collapse": True},
    {"field": "authorizedBy", "type": "list-facet", "collapse": True, "hidden": True},
    # for spatial query
    {
        "label": "Spatial Query",
        "field": "producedBy_samplingSite_location_rpt",
        "type": "spatialquery",
    },
]


# Define the scrape_data function
async def scrape_data():
    async with async_playwright() as p:
        # Launching the browser
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # Navigating to the URL
        await page.goto("https://central.isample.xyz/isamples_central/ui")

        # Wait for the necessary selector to load
        await page.wait_for_selector("#app div.solr-search-results ul.list-group li")

        # Extracting data
        results = await page.evaluate(
            """() => {
            const section = document.querySelector("#app div.solr-search-results ul.list-group li");
            const elements = section.querySelectorAll("ul li label");
            return Array.from(elements, element => element.innerText);
        }"""
        )

        # Closing the browser
        await browser.close()

        # Step 1: Convert fields array to an object for efficient lookup
        # grab label if it exists, otherwise use field name
        # fields_map = {field['label']: field for field in fields if 'label' in field}
        fields_map = {
            field.get("label", field.get("field").lower()).lower(): field
            for field in fields
        }

        # Step 2: Iterate over results to create the desired object
        # result_object = {result: fields_map[result] for result in results if result in fields_map}
        result_object = {
            result: fields_map.get(result.lower(), None) for result in results
        }

        # Outputting the results
        print(result_object)


# Run the scrape_data function
asyncio.run(scrape_data())
