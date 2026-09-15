"""
Ashkelon CTD chlorophyll against the satellite pixel over the same water.

Three stations were profiled off the Ashkelon desalination intake during the
September 2026 algae event. Each cast records chlorophyll fluorescence every
0.25 m down the water column; at a handful of depths a bottle was also drawn and
the chlorophyll extracted in the lab. The satellite sees only the skin of that
column, as one number per pixel per day, so the page puts the two side by side:
grey bars for the pixel each station sits in, dots for what the ship measured.

The CTD file lives outside the repository - it is not ours to publish - so this
script reads it from wherever it is and writes a self-contained page.

RUN:
    python ashkelon_ctd.py [path/to/ctd.csv]

OUTPUT:
    ../data/ashkelon_sat_chl.csv   satellite chlorophyll at the three pixels
    ../docs/ashkelon_ctd.html      the page
"""

import datetime as dt
import json
import pathlib
import sys

import numpy as np
import pandas as pd

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
DOCS = HERE.parent / "docs"
SAT = DATA / "ashkelon_sat_chl.csv"
OUT = DOCS / "ashkelon_ctd.html"
DEFAULT_CTD = pathlib.Path.home() / "Documents" / \
    "Algae_event_Desal_Ashkelon_CTD_Cal_fluo_DNA.csv"

TIME_COL = "DD MMM YYYY HH:MM:SS"   # the header lies; the cells are DD/MM/YYYY HH:MM
# Station -> (lat, lon, km from shore, seafloor depth). Ordered from the beach
# outwards, so reading down the page is swimming out to sea. The number in the
# field name is the bottom depth; the distances are to the OSM coastline, from
# shore_angle.nearest_shore_osm, which follows the Ashkelon breakwaters that the
# Natural Earth line cuts across (it puts the inshore station 0.5 km closer).
SITES = {
    "Ashkelon10": (31.681028, 34.547806, 0.7, 10),
    "Ashkelon23": (31.686931, 34.526092, 2.8, 23),
    "Ashkelon33": (31.691472, 34.509389, 4.4, 33),
}


# A fortnight either side of the survey, enough to see the bloom rise and fall.
SAT_START, SAT_END = "2026-08-20", "2026-09-14"
BOX = (34.46, 34.60, 31.64, 31.74)

# Depth is a magnitude sampled every 0.25 m, so it gets a continuous one-hue ramp
# and a colour bar rather than categorical hues - light at the surface, dark at
# the bottom, in both modes.
RAMP = {
    "light": ["#6fb0dd", "#4590cb", "#2170ae", "#0f5287", "#0a3760"],
    "dark": ["#b8dcf3", "#8fc3e2", "#6aa7cc", "#4c89b1", "#39698f"],
}
DEPTH_MAX = 33.0


def site_label(name):
    _, _, km, floor = SITES[name]
    return f"{km:g} km offshore · {floor} m bottom"


def satellite(refresh=False):
    """Daily chlorophyll in the pixel each station sits in."""
    if SAT.exists() and not refresh:
        return pd.read_csv(SAT)

    import xarray as xr
    import copernicusmarine as cm
    from chl_transects import SENSORS, record_end, CREDS

    def grab(dataset, start, end):
        return cm.open_dataset(
            dataset_id=dataset, variables=["CHL"],
            minimum_longitude=BOX[0], maximum_longitude=BOX[1],
            minimum_latitude=BOX[2], maximum_latitude=BOX[3],
            start_datetime=f"{start}T00:00:00", end_datetime=f"{end}T00:00:00",
            **CREDS).load()["CHL"]

    # The reprocessed stream stops a couple of weeks back; the near-real-time one
    # carries the rest. Same 1 km grid, so they concatenate cleanly.
    my_end, parts = record_end("multi1km"), []
    if SAT_START <= min(SAT_END, my_end):
        parts.append(grab(SENSORS["multi1km"]["dataset"],
                          SAT_START, min(SAT_END, my_end)))
    if SAT_END > my_end:
        first = (dt.date.fromisoformat(my_end) + dt.timedelta(days=1)).isoformat()
        s, e = max(SAT_START, first), min(SAT_END, record_end("nrt1km"))
        if s <= e:
            parts.append(grab(SENSORS["nrt1km"]["dataset"], s, e))
    f = (xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]).sortby("time")

    lats, lons, cube = f.latitude.values, f.longitude.values, f.values
    out = {"date": [str(t)[:10] for t in pd.to_datetime(f.time.values)]}
    for name, (la, lo, _, _) in SITES.items():
        i = int(np.abs(lats - la).argmin())
        j = int(np.abs(lons - lo).argmin())
        out[name] = np.round(cube[:, i, j], 3)
    d = pd.DataFrame(out)
    DATA.mkdir(exist_ok=True)
    d.to_csv(SAT, index=False)
    print(f"  wrote {SAT.name}: {len(d)} days, {d.date.iloc[0]} -> {d.date.iloc[-1]}")
    return d


def casts(path):
    """Daily median chlorophyll per station per depth, both measures."""
    f = pd.read_csv(path)
    f.columns = [c.strip() for c in f.columns]
    f["t"] = pd.to_datetime(f[TIME_COL], dayfirst=True, errors="coerce")
    f = f.dropna(subset=["t", "site", "DepSM"])
    f["date"] = f["t"].dt.date.astype(str)

    series = {}
    for col, key in (("Cal_Fluorescence", "ctd"), ("Ext_Chla_", "bottle")):
        d = f.dropna(subset=[col])
        # A station is usually cast once a day, but where a depth was revisited
        # the day's reading is the median of the visits.
        g = (d.groupby(["site", "date", "DepSM"])
              .agg(v=(col, "median"), n=(col, "size"))
              .reset_index().sort_values(["site", "date", "DepSM"]))
        series[key] = g
        print(f"  {key:<7}{len(d):>4} rows -> {len(g)} daily points, "
              f"{d['date'].nunique()} dates, "
              f"{g['DepSM'].nunique()} depths")
    return series


def page(series, sat):
    dates = list(sat["date"])
    sites = list(SITES)
    traces, shown = [], set()

    # One panel per station, bars and dots on the same axis so the two can be
    # read against each other directly. They are the same quantity in the same
    # water, and the gap between them is the point.
    def axes(i):
        n = "" if i == 0 else str(i + 1)
        return f"x{n}", f"y{n}"

    for i, site in enumerate(sites):
        xa, ya = axes(i)
        traces.append({
            "type": "bar", "x": dates,
            "y": [None if pd.isna(v) else float(v) for v in sat[site]],
            "name": "Satellite pixel", "legendgroup": "sat",
            "showlegend": i == 0, "xaxis": xa, "yaxis": ya,
            "marker": {"color": "var-bar"},
            "hovertemplate": "%{x|%-d %b}<br>satellite %{y:.1f} µg/L<extra></extra>",
        })

    for i, site in enumerate(sites):
        xa, ya = axes(i)
        for key, label, opts in (
            ("ctd", "CTD fluorescence",
             {"symbol": "circle-open", "size": 8, "line": {"width": 1.6}}),
            ("bottle", "Extracted (bottle)",
             {"symbol": "circle", "size": 12}),
        ):
            p = series[key]
            p = p[p["site"] == site]
            if p.empty:
                continue
            marker = dict(opts)
            marker["color"] = [round(float(d), 2) for d in p["DepSM"]]
            marker["colorscale"] = "var-ramp"
            marker["cmin"], marker["cmax"] = 0, DEPTH_MAX
            # One colour bar for the whole page, hung off the first trace that
            # carries the scale.
            marker["showscale"] = (i == 0 and key == "ctd")
            if marker["showscale"]:
                marker["colorbar"] = {
                    "title": {"text": "depth, m", "side": "top"},
                    "len": 0.93, "thickness": 11, "x": 1.005, "xanchor": "left",
                    "y": 0.5, "yanchor": "middle", "outlinewidth": 0,
                    "tickvals": [0, 10, 20, 30], "ticks": "outside",
                    "ticklen": 4,
                }
            traces.append({
                "type": "scatter", "mode": "markers",
                "x": list(p["date"]), "y": [round(float(v), 3) for v in p["v"]],
                "name": label, "legendgroup": key,
                "showlegend": key not in shown,
                "xaxis": xa, "yaxis": ya, "marker": marker,
                "customdata": [[label, round(float(d), 2), int(n)]
                               for d, n in zip(p["DepSM"], p["n"])],
                "hovertemplate": ("%{x|%-d %b} · %{customdata[0]}<br>"
                                  "%{y:.2f} µg/L at %{customdata[1]:.2f} m"
                                  "<extra></extra>"),
            })
            shown.add(key)

    # One range across all three panels, so the rows can be read against each
    # other rather than each against itself.
    top = float(np.nanmax(sat[sites].to_numpy(dtype=float)))
    meta = {
        "traces": traces,
        "sites": [site_label(s) for s in sites],
        "ramp": RAMP,
        # Half a day of padding so the first and last bars are not cut in half.
        "x": [(pd.Timestamp(dates[0]) - pd.Timedelta(hours=12)).isoformat(),
              (pd.Timestamp(dates[-1]) + pd.Timedelta(hours=12)).isoformat()],
        "yRange": [0, round(top * 1.06, 0)],
    }
    html = TEMPLATE.replace("__DATA__", json.dumps(meta, separators=(",", ":")))
    DOCS.mkdir(exist_ok=True)
    OUT.write_text(html)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.0f} kB, "
          f"{len(traces)} traces)")


TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ashkelon &mdash; chlorophyll through the water column</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
<style>
:root, .viz-root {
  --page: #fcfcfb; --ink: #1a1a19; --ink-2: #57564f; --ink-3: #83827d;
  --grid: #e5e4df; --bar: #d3d2cc;
}
@media (prefers-color-scheme: dark) {
  :root, .viz-root {
    --page: #1a1a19; --ink: #f5f4ee; --ink-2: #b7b6ae; --ink-3: #83827d;
    --grid: #33322e; --bar: #46453f;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 24px 20px 40px; background: var(--page); color: var(--ink);
  font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}
.wrap { max-width: 900px; margin: 0 auto; }
h1 { font-size: 21px; font-weight: 600; margin: 0 0 6px; }
p.sub { margin: 0 0 4px; color: var(--ink-2); font-size: 14px; }
p.note { margin: 14px 0 0; color: var(--ink-3); font-size: 13px; }
a { color: inherit; }
#chart { width: 100%; height: 820px; }
.links { margin-top: 18px; font-size: 13px; color: var(--ink-3); }
.links a { margin-right: 14px; }
</style>
</head>
<body class="viz-root">
<div class="wrap">
  <h1>Ashkelon &mdash; chlorophyll through the water column</h1>
  <p class="sub">Three CTD stations off the desalination intake, profiled during
  the September 2026 algae event, against the satellite pixel each one sits in.
  Stations are named by their distance from the coastline.</p>
  <p class="sub">Open rings are the CTD's own fluorescence sensor, filled dots the
  chlorophyll extracted from a bottle in the lab; colour is depth.</p>
  <div id="chart"></div>
  <p class="note">The bars and the dots measure the same thing in the same water
  and disagree by a factor of several. The satellite integrates a 1&nbsp;km pixel
  over the top metre or so of a turbid, shallow coastal strip, where suspended
  sediment and coloured dissolved matter are read as chlorophyll; the ship samples
  a column that turns out to be almost uniform, falling only from about
  4.7&nbsp;µg/L in the top 10&nbsp;m to 3.3 below 25 at the deepest station.</p>
  <div class="links">
    <a href="chl_seasonal.html">Chlorophyll map</a>
    <a href="chl_buoy.html">Buoy vs satellite</a>
    <a href="sst_seasonal.html">Sea temperature map</a>
    <a href="index.html">All charts</a>
  </div>
</div>
<script>
const META = __DATA__;

function css(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}
function dark() {
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}

// Colours are resolved at draw time so the ramp can follow the colour scheme.
function ramp() {
  const steps = META.ramp[dark() ? "dark" : "light"];
  return steps.map((c, i) => [i / (steps.length - 1), c]);
}
function paint(trace) {
  const t = JSON.parse(JSON.stringify(trace));
  const m = t.marker;
  if (!m) return t;
  if (m.color === "var-bar") m.color = css("--bar");
  if (m.colorscale === "var-ramp") m.colorscale = ramp();
  if (m.colorbar) {
    m.colorbar.title.font = { color: css("--ink-2"), size: 12 };
    m.colorbar.tickfont = { color: css("--ink-2"), size: 11 };
  }
  // A 2px surface ring keeps the filled bottle dots legible on top of the cast.
  if (m.symbol === "circle") m.line = { width: 2, color: css("--page") };
  if (m.symbol === "circle-open" && m.line) m.line.color = undefined;
  return t;
}

const SITE_GAP = 0.055;

function layout() {
  const ink = css("--ink"), ink2 = css("--ink-2"), grid = css("--grid");
  const n = META.sites.length;
  const top = 0.995, floor = 0.045;
  const h = (top - floor - SITE_GAP * (n - 1)) / n;
  const lay = {
    paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: ink2, size: 12 },
    margin: { l: 62, r: 80, t: 8, b: 34 },
    bargap: 0.3, hovermode: "closest",
    hoverlabel: { bgcolor: css("--page"), bordercolor: grid,
                  font: { color: ink, size: 12 } },
    showlegend: true,
    legend: {
      orientation: "h", x: 0, y: -0.045, xanchor: "left", yanchor: "top",
      font: { color: ink2, size: 12 }, itemsizing: "constant",
      bgcolor: "rgba(0,0,0,0)",
    },
    annotations: [],
  };
  META.sites.forEach((site, i) => {
    const ax = i === 0 ? "" : String(i + 1);
    const y1 = top - i * (h + SITE_GAP), y0 = y1 - h;
    lay["xaxis" + ax] = {
      type: "date", range: META.x, anchor: "y" + ax, domain: [0, 1],
      showgrid: false, zeroline: false, linecolor: grid, tickcolor: grid,
      tickfont: { color: ink2, size: 11 }, showticklabels: i === n - 1,
      tickformat: "%-d %b", dtick: 86400000 * 3,
    };
    lay["yaxis" + ax] = {
      domain: [y0, y1], anchor: "x" + ax, range: META.yRange,
      title: { text: i === 1 ? "chlorophyll a, µg/L" : "",
               font: { color: ink2, size: 12 }, standoff: 8 },
      gridcolor: grid, zeroline: false, linecolor: grid,
      tickfont: { color: ink2, size: 11 }, dtick: 10,
    };
    lay.annotations.push({
      xref: "paper", yref: "paper", x: 0.004, y: y1 - 0.004,
      xanchor: "left", yanchor: "top", showarrow: false, text: site,
      font: { color: ink, size: 13 },
    });
  });
  return lay;
}

function draw() {
  Plotly.react("chart", META.traces.map(paint), layout(),
               { displayModeBar: false, responsive: true });
}
draw();
window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", draw);
</script>
</body>
</html>
"""


def main(argv):
    path = pathlib.Path(argv[0]) if argv else DEFAULT_CTD
    if not path.exists():
        raise SystemExit(f"no CTD file at {path}")
    print(f"reading {path.name}")
    series = casts(path)
    print("satellite chlorophyll at the three pixels")
    sat = satellite()
    page(series, sat)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
