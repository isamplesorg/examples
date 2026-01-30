const playwright = require('playwright');

const MIN_YEAR = 1800;
const MAX_YEAR = new Date().getFullYear();

const fields = [
    { field: "curation_accessContraints", type: "non-search", hidden: true },
    { field: "curation_description_text", type: "non-search", hidden: true },
    { field: "curation_label", type: "non-search", hidden: true },
    { field: "curation_location", type: "non-search", hidden: true },
    { field: "curation_responsibility", type: "non-search", hidden: true },
    { field: "description_text", type: "non-search", hidden: true },
    { label: "Context", field: "hasContextCategory", type: "hierarchy-facet", collapse: true },
    { label: "Material", field: "hasMaterialCategory", type: "hierarchy-facet", collapse: true },
    { label: "Specimen", field: "hasSpecimenCategory", type: "hierarchy-facet", collapse: true },
    { label: "Identifier", field: "id", type: "text" },
    { field: "informalClassification", type: "non-search", hidden: true },
    { field: "keywords", type: "text" },
    { field: "label", type: "non-search" },
    { field: "producedBy_description_text", type: "non-search", hidden: true },
    { field: "producedBy_hasFeatureOfInterest", type: "non-search", hidden: true },
    { field: "producedBy_label", type: "non-search", hidden: true },
    { field: "producedBy_responsibility", type: "non-search", hidden: true },
    { field: "producedBy_resultTime", type: "non-search" },
    { label: "Collection Date", field: "producedBy_resultTimeRange", type: "date-range-facet", minValue: MIN_YEAR, maxValue: MAX_YEAR, value: [MIN_YEAR, MAX_YEAR] },
    { field: "producedBy_samplingSite_description_text", type: "non-search", hidden: true },
    { field: "producedBy_samplingSite_label", type: "non-search", hidden: true },
    { field: "producedBy_samplingSite_location_elevationInMeters", type: "non-search", hidden: true },
    { field: "producedBy_samplingSite_location_latitude", type: "non-search", hidden: true },
    { field: "producedBy_samplingSite_location_longitude", type: "non-search", hidden: true },
    { field: "producedBy_samplingSite_placeName", type: "non-search" },
    { field: "registrant", type: "list-facet", facetSort: "count", collapse: true },
    { field: "samplingPurpose", type: "non-search", hidden: true },
    { label: "All text fields", field: "searchText", type: "text" },
    { field: "source", type: "list-facet", facetSort: "index", collapse: true },
    { field: "sourceUpdatedTime", type: "non-search", collapse: true },
    { field: "authorizedBy", type: "list-facet", collapse: true, hidden: true },
    // for spatial query
    { label: "Spatial Query", field: "producedBy_samplingSite_location_rpt", type: "spatialquery" },
];


async function scrapeData() {
    // Launching the browser
    const browser = await playwright.chromium.launch();
    const context = await browser.newContext();
    const page = await context.newPage();

    // Navigating to the URL
    await page.goto('https://central.isample.xyz/isamples_central/ui');

    // Wait for the necessary selector to load
    await page.waitForSelector("#app div.solr-search-results ul.list-group li");

    // Extracting data
    const results = await page.evaluate(() => {
        const section = document.querySelector("#app div.solr-search-results ul.list-group li");
        const elements = section.querySelectorAll("ul li label");
        return Array.from(elements, element => element.innerText);
    });

    // Closing the browser
    await browser.close();


    // Step 1: Convert fields array to an object for efficient lookup
    const fieldsMap = fields.reduce((acc, field) => {
        if (field.label) {
            acc[field.label] = field;
        }
        return acc;
    }, {});

    // Step 2: Iterate over results to create the desired object
    const resultObject = results.reduce((acc, result) => {
        if (fieldsMap[result]) {
            acc[result] = fieldsMap[result];
        }
        return acc;
    }, {});


    // Outputting the results
    const jsonOutput = JSON.stringify(resultObject, null, 2);
    console.log(jsonOutput);
}

scrapeData();
