"""A local page for judging PACE chlorophyll against the maps the site already uses.

PACE carries the newest ocean-colour sensor, measuring in about a hundred bands
where the others use six, and the question is whether that shows over our coast.
The only fair way to look is the same day, the same colour scale and the same
frame, so this writes PACE's own measurement out as a map overlay and puts it
beside the Copernicus fields, which the page loads live from their tile service
using an identical ramp (viridis, log, 0.01 to 10 mg m-3).

    python code/pace_view.py                       # the dates moana_fetch used
    python code/pace_view.py 2026-08-05 2026-09-10

The page and its images are written to ~/Documents/pace_view/, outside the
repository: this is for looking, not for publishing. The NetCDF files come from
the cache moana_fetch.py fills, and are downloaded if they are not there yet.
"""
import json
import os
import sys

import numpy as np
import xarray as xr
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from moana_fetch import EarthdataSession, credentials, fetch   # noqa: E402

OUT = os.path.expanduser("~/Documents/pace_view")
DATES = ["2026-08-05", "2026-08-20"]
# The frame the sea maps on the site open with, so a screenshot lines up.
LAT0, LAT1 = 30.5, 33.6
LON0, LON1 = 29.5, 35.8
# The chlorophyll scale the site uses, so the same water is the same colour.
VMIN, VMAX = 0.01, 10.0
WIDTH = 1400          # px across the box; the source is 4 km, so this is generous


def viridis(t):
    """Viridis without pulling in matplotlib: its 8 anchor colours, interpolated."""
    anchors = np.array([
        [68, 1, 84], [72, 40, 120], [62, 74, 137], [49, 104, 142],
        [38, 130, 142], [31, 158, 137], [53, 183, 121], [109, 205, 89],
        [180, 222, 44], [253, 231, 37]], dtype=float)
    x = np.clip(t, 0, 1) * (len(anchors) - 1)
    lo = np.floor(x).astype(int)
    hi = np.minimum(lo + 1, len(anchors) - 1)
    f = (x - lo)[..., None]
    return (anchors[lo] * (1 - f) + anchors[hi] * f).astype(np.uint8)


def mercator_y(lat):
    return np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))


def overlay(da, path):
    """The field as a PNG in web mercator, transparent where nothing was measured.

    Leaflet stretches an image overlay linearly between its corners in mercator,
    so the rows have to be resampled into mercator first; laying a plate carree
    image straight onto the map would put the coast a few kilometres out.
    """
    lat = da.lat.values
    lon = da.lon.values
    field = da.values
    if lat[0] > lat[-1]:                      # north-up in the file
        lat, field = lat[::-1], field[::-1]
    height = int(round(WIDTH * (mercator_y(LAT1) - mercator_y(LAT0))
                       / np.radians(LON1 - LON0)))
    ys = np.linspace(mercator_y(LAT1), mercator_y(LAT0), height)
    rows = np.interp(np.degrees(2 * np.arctan(np.exp(ys)) - np.pi / 2), lat,
                     np.arange(len(lat)))
    cols = np.interp(np.linspace(LON0, LON1, WIDTH), lon, np.arange(len(lon)))
    grid = field[np.clip(np.round(rows), 0, len(lat) - 1).astype(int)][:,
                 np.clip(np.round(cols), 0, len(lon) - 1).astype(int)]

    ok = np.isfinite(grid)
    t = (np.log10(np.where(ok, grid, VMIN).clip(VMIN, VMAX)) - np.log10(VMIN)) \
        / (np.log10(VMAX) - np.log10(VMIN))
    rgba = np.zeros(grid.shape + (4,), dtype=np.uint8)
    rgba[..., :3] = viridis(t)
    rgba[..., 3] = np.where(ok, 255, 0)
    Image.fromarray(rgba, "RGBA").save(path)
    return dict(measured_pct=round(100 * ok.mean(), 1),
                median=round(float(np.median(grid[ok])), 3) if ok.any() else None,
                p90=round(float(np.percentile(grid[ok], 90)), 3) if ok.any() else None,
                pixels=int(ok.sum()))


def build(days):
    os.makedirs(OUT, exist_ok=True)
    creds = credentials()
    if not creds:
        sys.exit("No Earthdata credentials; see code/moana_fetch.py.")
    session = EarthdataSession(creds)
    stats = {}
    for day in days:
        path = fetch("chl", day, session)
        if not path:
            print(f"{day}: no PACE file")
            continue
        with xr.open_dataset(path, engine="h5netcdf") as ds:
            if "chlor_a" not in ds:
                print(f"{day}: no chlor_a in {os.path.basename(path)}")
                continue
            da = ds["chlor_a"].sel(lat=slice(LAT1, LAT0), lon=slice(LON0, LON1)).load()
        stats[day] = overlay(da, os.path.join(OUT, f"pace_chl_{day}.png"))
        print(f"{day}: {stats[day]['measured_pct']}% measured, "
              f"median {stats[day]['median']} mg m-3")
    page(days, stats)


def page(days, stats):
    """One map, one day at a time, and a switch between the three products."""
    html = HTML.replace("__DAYS__", json.dumps(days)) \
               .replace("__STATS__", json.dumps(stats)) \
               .replace("__BOX__", json.dumps([[LAT0, LON0], [LAT1, LON1]])) \
               .replace("__VMIN__", str(VMIN)).replace("__VMAX__", str(VMAX))
    out = os.path.join(OUT, "index.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n{out}\nopen it with:  xdg-open {out}")


HTML = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<title>כלורופיל: PACE מול קופרניקוס</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  html, body { margin:0; height:100%; font-family:Arial, sans-serif; }
  #map { position:absolute; inset:0; direction:ltr; }
  .bar { position:absolute; z-index:500; top:8px; left:50%; transform:translateX(-50%);
    background:#fff; border:1px solid rgba(0,0,0,.15); border-radius:9px;
    box-shadow:0 2px 8px rgba(0,0,0,.15); padding:7px 10px; display:flex;
    gap:8px; align-items:center; flex-wrap:wrap; max-width:94vw; }
  button, select { font:inherit; font-size:13px; padding:4px 9px; cursor:pointer;
    border:1px solid rgba(0,0,0,.15); border-radius:7px; background:#fff; }
  button.on { font-weight:700; border-color:#666; }
  .note { position:absolute; z-index:500; bottom:8px; left:50%;
    transform:translateX(-50%); background:#fff; border:1px solid rgba(0,0,0,.15);
    border-radius:9px; padding:6px 10px; font-size:12.5px; max-width:94vw; }
  .ramp { width:170px; height:11px; border:1px solid rgba(0,0,0,.25);
    background:linear-gradient(to right,#440154,#482878,#3e4a89,#31688e,#26828e,
      #1f9e89,#35b779,#6dcd59,#b4de2c,#fde725); }
</style>
</head>
<body>
<div id="map"></div>
<div class="bar">
  <span>יום</span><select id="day"></select>
  <span style="margin-right:6px">מפה</span>
  <button class="src on" data-src="pace">PACE 4 ק"מ</button>
  <button class="src" data-src="cmems1km">קופרניקוס 1 ק"מ</button>
  <button class="src" data-src="olci300">OLCI 300 מ'</button>
  <span style="margin-right:6px">אטימות</span>
  <input type="range" id="op" min="0" max="100" value="100" style="width:90px">
  <span class="ramp" title="0.01 עד 10 מ״ג/מ״ק, לוגריתמי"></span>
</div>
<div class="note" id="note"></div>
<script>
const DAYS = __DAYS__, STATS = __STATS__, BOX = __BOX__;
const VMIN = __VMIN__, VMAX = __VMAX__;
// The same ramp and the same limits for all three, which is the whole point:
// any difference on the screen is the measurement, not the colouring.
const STYLE = `cmap:viridis,logScale,range:${VMIN}/${VMAX}`;
const CM = "https://wmts.marine.copernicus.eu/teroWmts";
const LAYERS = {
  cmems1km: "OCEANCOLOUR_MED_BGC_L3_MY_009_143/"
    + "cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D_202411/CHL",
  olci300: "OCEANCOLOUR_MED_BGC_L3_MY_009_143/"
    + "cmems_obs-oc_med_bgc-plankton_my_l3-olci-300m_P1D_202211/CHL",
};
let src = "pace", day = DAYS[0], layer = null;

const map = L.map("map").fitBounds(BOX);
L.tileLayer("https://mt1.google.com/vt/lyrs=m&hl=iw&x={x}&y={y}&z={z}",
  {maxZoom: 18, attribution: "מפות Google"}).addTo(map);

function draw() {
  if (layer) { map.removeLayer(layer); layer = null; }
  const opacity = +document.getElementById("op").value / 100;
  if (src === "pace") {
    layer = L.imageOverlay(`pace_chl_${day}.png`, BOX, {opacity}).addTo(map);
  } else {
    layer = L.tileLayer(`${CM}?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetTile`
      + `&LAYER=${encodeURIComponent(LAYERS[src])}&STYLE=${encodeURIComponent(STYLE)}`
      + `&TILEMATRIXSET=EPSG:3857&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}`
      + `&FORMAT=image/png&TIME=${day}T00:00:00.000Z`,
      {maxZoom: 12, opacity, attribution: "E.U. Copernicus Marine"}).addTo(map);
  }
  const s = STATS[day] || {};
  const what = {pace: "PACE OCI, 4 ק\\"מ, חיישן היפרספקטרלי",
                cmems1km: "קופרניקוס רב-חיישני, 1 ק\\"מ",
                olci300: "קופרניקוס OLCI, 300 מ'"}[src];
  document.getElementById("note").textContent =
    `${what} · ${day} · סקאלה זהה לשלושתם: ${VMIN}–${VMAX} מ״ג/מ״ק, לוגריתמי`
    + (src === "pace" && s.measured_pct !== undefined
        ? ` · נמדדו ${s.measured_pct}% מהמסגרת, חציון ${s.median}` : "");
}

const sel = document.getElementById("day");
DAYS.forEach(d => { const o = document.createElement("option"); o.value = o.textContent = d; sel.appendChild(o); });
sel.onchange = e => { day = e.target.value; draw(); };
document.querySelectorAll("button.src").forEach(b => b.onclick = () => {
  src = b.dataset.src;
  document.querySelectorAll("button.src").forEach(o => o.classList.toggle("on", o === b));
  draw();
});
document.getElementById("op").oninput = () => { if (layer) layer.setOpacity(+event.target.value / 100); };
draw();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    build(sys.argv[1:] or DATES)
