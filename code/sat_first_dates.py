"""First day each satellite layer of docs/sat_browser.html actually has data.

The browser needs to know where a map's record begins: "התחלה" jumps there, and
the date box refuses anything earlier. The answer is published by both services
in their WMTS capabilities, so it is read from there rather than guessed.

The date the capabilities give is where the product is declared to start, and
that is a day or two before the first tile the service will actually draw: the
1 km sea-temperature analysis says 1 January 2008 and answers 400 for it, while
2 January comes back fine. So each date is checked by asking for one tile and
walking forward until one arrives painted.

Two kinds of answer come back:
  - a fixed date, for an archive that keeps growing from a first day;
  - a rolling window, for the Copernicus near-real-time ocean-colour products,
    which keep about twelve days and drop the rest. Those are written as a
    number of days back from today, since a date would be stale tomorrow.

    python code/sat_first_dates.py        # refresh data/sat_first_dates.json

The capabilities are large (66 MB for Copernicus), so this is a job to run when
a layer is added or a product is republished, not on a schedule.
"""
import io
import json
import math
import re
import urllib.parse
import urllib.request
from datetime import date, timedelta

from PIL import Image

CAPS = {
    "cm": "https://wmts.marine.copernicus.eu/teroWmts"
          "?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetCapabilities",
    "gibs": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/1.0.0/WMTSCapabilities.xml",
}
PAGE = "docs/sat_browser.html"
OUT = "data/sat_first_dates.json"
# A record that starts this recently is not an archive with a start, it is a
# window that moves with today.
ROLLING_DAYS = 60
# How far past the declared start to look for the first tile with something on
# it. The early ocean-colour years are thin - SeaWiFS passed over this corner of
# the basin every few days at best - so this has to reach past a month.
MAX_WALK = 120
# Where the question is asked: the sea off Haifa, off Tel Aviv and off Ashkelon.
# A day can carry a swath elsewhere in the basin and miss this coast entirely,
# and it is this coast the page is about, so a day counts only if one of these
# three has something on it. The same three points the page itself probes.
PROBE_PTS = [(32.90, 34.90), (32.14, 34.66), (31.60, 34.45)]
PROBE_Z = 8
PAINTED = 0.01
TILE = {
    "cm": "https://wmts.marine.copernicus.eu/teroWmts?SERVICE=WMTS&VERSION=1.0.0"
          "&REQUEST=GetTile&LAYER={layer}&STYLE=cmap%3Aviridis&TILEMATRIXSET=EPSG:3857"
          "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}&FORMAT=image/png"
          "&TIME={day}T00:00:00.000Z",
    "gibs": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/{layer}/default/{day}"
            "/GoogleMapsCompatible_Level{lvl}/{z}/{y}/{x}.{ext}",
}


def tile_xy(lat, lon, z):
    n = 2 ** z
    x = int((lon + 180) / 360 * n)
    s = math.sin(math.radians(lat))
    return x, int((0.5 - math.log((1 + s) / (1 - s)) / (4 * math.pi)) * n)


def painted(service, layer, day, lvl, ext, lat, lon):
    """Fraction of one probe tile that came back with something drawn on it."""
    z = min(PROBE_Z, int(lvl)) if service == "gibs" else PROBE_Z
    x, y = tile_xy(lat, lon, z)
    url = TILE[service].format(layer=urllib.parse.quote(layer) if service == "cm" else layer,
                               z=z, x=x, y=y, day=day, lvl=lvl, ext=ext)
    try:
        raw = urllib.request.urlopen(url, timeout=60).read()
        im = Image.open(io.BytesIO(raw)).convert("RGBA")
        px = list(im.getdata())
    except Exception:
        return 0.0
    # A field is drawn on transparency, so alpha is the answer. Asking about
    # darkness too would throw real data away, since the turbid and algae ramps
    # start near black. Imagery is a jpeg with no transparency, and there an
    # empty tile is solid black while a real one never is.
    jpeg = ext == "jpg"
    on = sum(1 for r, g, b, a in px
             if a > 10 and (not jpeg or r > 12 or g > 12 or b > 12))
    return on / len(px)


def has_data(service, layer, day, lvl, ext):
    """Whether this day has anything along our own coast."""
    return any(painted(service, layer, day, lvl, ext, lat, lon) > PAINTED
               for lat, lon in PROBE_PTS)


def first_with_data(service, layer, start, lvl, ext):
    """Walk forward from the declared start to the first day that draws."""
    day = date.fromisoformat(start)
    for _ in range(MAX_WALK):
        if has_data(service, layer, day.isoformat(), lvl, ext):
            return day.isoformat()
        day += timedelta(days=1)
    return None


def page_layers():
    """[(id, service, layer identifier, matrix level, extension)] from the page."""
    html = open(PAGE, encoding="utf-8").read()
    block = re.search(r"const LAYERS = \[.*?\n\];", html, re.S).group(0)
    ids = re.findall(r'\{id:"([^"]+)"', block)
    srcs = re.findall(r'src:"(cm|gibs)"', block)
    # a layer path is written over one or two lines
    layers = [a + (b or "") for a, b in
              re.findall(r'\n   layer:"([^"]+)"(?:\s*\n\s*\+ "([^"]*)")?', block)]
    assert len(ids) == len(srcs) == len(layers), "the page did not parse cleanly"
    out = []
    for lid, src, lay in zip(ids, srcs, layers):
        one = re.search(r'\{id:"' + lid + r'".*?(?=\{id:"|\Z)', block, re.S).group(0)
        lvl = re.search(r"GoogleMapsCompatible_Level(\d)", one)
        ext = re.search(r'ext:"(\w+)"', one)
        out.append((lid, src, lay, lvl.group(1) if lvl else "9",
                    ext.group(1) if ext else "png"))
    return out


def first_dates(caps, service, wanted):
    """{layer identifier: first date} for the identifiers asked about."""
    out = {}
    if service == "gibs":
        for chunk in re.split(r"<Layer>", caps):
            m = re.search(r"<ows:Identifier>([^<]+)</ows:Identifier>", chunk)
            if not m or m.group(1) not in wanted:
                continue
            values = re.findall(r"<Value>([^<]+)</Value>", chunk)
            if values:
                out[m.group(1)] = values[0].split("/")[0]
        return out
    for layer in wanted:
        i = caps.find(f"<ows:Identifier>{layer}</ows:Identifier>")
        if i < 0:
            continue
        # the time dimension follows the identifier; its first value is the start
        for value in re.findall(r"<Value>([^<]+)</Value>", caps[i:i + 40000]):
            m = re.match(r"(\d{4}-\d{2}-\d{2})", value)
            if m:
                out[layer] = m.group(1)
                break
    return out


def main():
    layers = page_layers()
    found = {}
    for service, url in CAPS.items():
        wanted = {lay for _, src, lay, _, _ in layers if src == service}
        print(f"reading {service} capabilities…")
        caps = urllib.request.urlopen(url, timeout=600).read().decode("utf-8", "replace")
        found.update(first_dates(caps, service, wanted))

    today = date.today()
    out = {}
    for lid, src, lay, lvl, ext in layers:
        declared = found.get(lay)
        if not declared:
            print(f"  {lid}: not in the capabilities, left out")
            continue
        real = first_with_data(src, lay, declared, lvl, ext)
        if not real:
            print(f"  {lid}: nothing drawn in {MAX_WALK} days from {declared}, "
                  f"keeping the declared date")
            real = declared
        age = (today - date.fromisoformat(real)).days
        if age <= ROLLING_DAYS:
            out[lid] = {"rolling": age}
            print(f"  {lid}: rolling window, {age} days")
        else:
            out[lid] = {"first": real}
            note = "" if real == declared else f"  (declared {declared})"
            print(f"  {lid}: from {real}{note}")

    with open(OUT, "w") as f:
        json.dump({"generated": today.isoformat(), "layers": out}, f, indent=1)
    print(f"{OUT}  {len(out)} layers")


if __name__ == "__main__":
    main()
