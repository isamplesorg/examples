/**
 * ESM source for Cesium widget example.
 * 
 * This example loads points into a Cesium viewer and provides mouse over feedback.
 * 
 * Intended for use as a MapWidget ESM.
 */

function loadScript(src) {
    return new Promise((resolve, reject) => {
        let script = Object.assign(document.createElement("script"), {
            type: "text/javascript",
            async: true,
            src: src,
        });
        script.addEventListener("load", resolve);
        script.addEventListener("error", reject);
        document.body.appendChild(script);
    });
};

await loadScript("https://cesium.com/downloads/cesiumjs/releases/1.103/Build/Cesium/Cesium.js");

const scaler = new Cesium.NearFarScalar(1.5e2, 3, 8.0e6, 1.0);

// Size of each point
const point_size = 10;

// number of points to load
const n_points = 100000;
// elevation of a point if missing
const MISSING_Z = 10.0

// The collection of points
let points = new Cesium.PointPrimitiveCollection();

// Add a property to points to track the selected point
points._selected = {p: null, c: new Cesium.Color()};

let heights = {};

let handler;
let pthandler;
let nameOverlay;

function addPoint(point_collection, pid, x, y, z, h310) {
    if (z == null) {
        z = MISSING_Z;
    }
    heights.h310 = (heights.h310 ??= z) + 10;
    point_collection.add({
        id: pid,
        position: Cesium.Cartesian3.fromDegrees(x, y, heights.h310),
        pixelSize: point_size,
        color: Cesium.Color.CYAN,
        scaleByDistance: scaler
    })
}

// load points from isb stream source
async function loadPoints(point_collection, src, n_points) {
    const params = new URLSearchParams({
        rows: n_points,
        fl: "id,x:producedBy_samplingSite_location_longitude,y:producedBy_samplingSite_location_latitude,z:producedBy_samplingSite_location_elevationInMeters,h310:producedBy_samplingSite_location_h3_12",
        q: "source:GEOME"
    });
    const headers = new Headers();
    headers.append("Origin", "localhost")
    const url = src + params;
    fetch(url, {"headers": headers})
        .then(response => response.json())
        .then(data => {
            data["result-set"].docs.forEach(function (doc) {
                addPoint(point_collection, doc.id, doc.x, doc.y, doc.z || MISSING_Z, doc.h310);
            });
        });
}

async function doLoadPoints() {
    //const src = "https://central.isample.xyz/isamples_central/thing/stream?";
    const src = "http://localhost:8010/proxy/isamples_central/thing/stream?";
    loadPoints(points, src, n_points);
}


// Deselect a point
function deselect(ptnameOverlay, point_collection) {
    if (point_collection._selected.p !== null) {
        ptnameOverlay.style.display = "none";
        point_collection._selected.p.primitive.color = point_collection._selected.c;
        point_collection._selected.p.primitive.pixelSize = point_size;
        point_collection._selected.p = null;
    }
}

// Select a point
function select(viewer, ptnameOverlay, pt, position, point_collection) {
    if (point_collection._selected.p !== null) {
        if (point_collection._selected.p.id === pt.id) {
            return;
        }
        deselect(nameOverlay, point_collection);
    }
    point_collection._selected.p = pt;
    Cesium.Color.clone(pt.primitive.color, point_collection._selected.c);
    point_collection._selected.p.primitive.color = Cesium.Color.YELLOW;
    point_collection._selected.p.primitive.pixelSize = 10;
    // Can also use this to transform feature coords to window
    // const window_pos = Cesium.SceneTransforms.wgs84ToWindowCoordinates(viewer.scene, pt.primitive.position);
    const toppos = viewer.canvas.getBoundingClientRect().top + window.scrollY;
    ptnameOverlay.style.display = "block";
    // 30 is the vertical offset of the top of the element to the position
    // Depends on the hight of the text and the border etc of the element
    
    //ptnameOverlay.style.top = `${toppos + position.y - 30}px`;
    ptnameOverlay.style.top = `${position.y - 30}px`;
    ptnameOverlay.style.left = `${position.x + 10}px`;
    
    const name = point_collection._selected.p.id;
    ptnameOverlay.textContent = name;
    console.log(ptnameOverlay)
}

function addCoordinateDisplay(viewer, scene) {
    const entity = viewer.entities.add({
        label: {
          show: false,
          showBackground: true,
          font: "14px monospace",
          horizontalOrigin: Cesium.HorizontalOrigin.LEFT,
          verticalOrigin: Cesium.VerticalOrigin.TOP,
          pixelOffset: new Cesium.Cartesian2(15, 0),
        },
      });
    // Mouse over the globe to see the cartographic position
    handler = new Cesium.ScreenSpaceEventHandler(scene.canvas);
    handler.setInputAction(function (movement) {
      const cartesian = viewer.camera.pickEllipsoid(
        movement.endPosition,
        scene.globe.ellipsoid
      );
      if (cartesian) {
        const cartographic = Cesium.Cartographic.fromCartesian(
          cartesian
        );
        const longitudeString = Cesium.Math.toDegrees(
          cartographic.longitude
        ).toFixed(2);
        const latitudeString = Cesium.Math.toDegrees(
          cartographic.latitude
        ).toFixed(2);

        entity.position = cartesian;
        entity.label.show = true;
        entity.label.text =
          `Lon: ${`   ${longitudeString}`.slice(-7)}\u00B0` +
          `\nLat: ${`   ${latitudeString}`.slice(-7)}\u00B0`;
      } else {
        entity.label.show = false;
      }
    }, Cesium.ScreenSpaceEventType.MOUSE_MOVE);
}

function addIdentifierDisplay(viewer, scene) {
    // Create an overlay for showing the point ID
    nameOverlay = document.createElement("div");
    nameOverlay.className = "backdrop";
    nameOverlay.style.display = "block";
    nameOverlay.style.position = "absolute";
    //nameOverlay.style.position = "relative";
    //nameOverlay.style.bottom = "0";
    nameOverlay.style.left = "100";
    nameOverlay.style.bottom = "100"
    nameOverlay.style["pointer-events"] = "none";
    nameOverlay.style.padding = "4px";
    nameOverlay.style.height = "30px";
    nameOverlay.style.backgroundColor = "black";
    nameOverlay.style.color = "white";
    nameOverlay.style.verticalAlign = "baseline";

    const updateRoi = false;
  
    viewer.camera.moveEnd.addEventListener(function () {
        if (updateRoi) {
          var rect = viewer.camera.computeViewRectangle();
          var east = String(Cesium.Math.toDegrees(rect.east));
          var south = String(Cesium.Math.toDegrees(rect.south));
          var west = String(Cesium.Math.toDegrees(rect.west));
          var north = String(Cesium.Math.toDegrees(rect.north));
          var coords = south + " " + west + " " + north + " " + east;
    
          console.log(
            JSON.stringify({ func: "updateROI", args: [coords] })
          );
        }
      });   


    viewer.container.appendChild(nameOverlay);

    // Handler responding to mouse move events
    pthandler = new Cesium.ScreenSpaceEventHandler(viewer.scene.canvas);
    pthandler.setInputAction(function (movement) {
        // Check what is under the mouse cursor
        var pickedObject = viewer.scene.pick(movement.endPosition);
        if (Cesium.defined(pickedObject)) {
            // if the feature is in the points collection...
            if (pickedObject.collection === points) {
                select(viewer, nameOverlay, pickedObject, movement.endPosition, points);
            }
        } else {
            deselect(nameOverlay, points);
        }
    }, Cesium.ScreenSpaceEventType.MOUSE_MOVE);

}


export function render(view) {

    let width = view.model.get("width");
    let height = view.model.get("height");

    const div = document.createElement("div");
    div.style.width = width;
    div.style.height = height;

    Cesium.Ion.defaultAccessToken = view.model.get("token");

    const viewer = new Cesium.Viewer(div, {
        //terrainProvider: Cesium.createWorldTerrain()
    });
    const scene = viewer.scene;

    viewer.scene.primitives.add(points);


    view.el.appendChild(div);
    setTimeout(addCoordinateDisplay, 100, viewer, scene);
    setTimeout(doLoadPoints, 200);
    setTimeout(addIdentifierDisplay, 2000, viewer, scene);
}
