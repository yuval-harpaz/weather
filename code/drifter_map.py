"""
Map the real (measured) surface drifters in the eastern Mediterranean: where the
live ones are right now, and where the historical ones went, against our model
sampling stations.

Two independent free sources, neither needing a login:
    NOAA OSMC   - last 30 days of every reporting platform, near real time.
    NOAA AOML   - Global Drifter Program, quality-controlled 6-hourly velocities
                  (delayed mode, so it stops a few years back).

RUN:
    python drifter_map.py

OUTPUT:
    drifter_map.html
"""

import json
import pathlib
import datetime as dt
import urllib.request

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from coast_points import POINTS

CACHE = pathlib.Path.home() / ".cache" / "weather"
OUTFILE = pathlib.Path(__file__).with_name("drifter_map.html")

# Eastern Mediterranean, wide enough to show what is out there at all.
BOX = dict(lat0=30.0, lat1=37.5, lon0=25.0, lon1=36.5)
# The corridor itself, for the historical fixes.
CORRIDOR = dict(lat0=30.9, lat1=33.0, lon0=33.3, lon1=35.3)
HIST_FROM = "2015-01-01T00:00:00Z"

OSMC = ("https://osmc.noaa.gov/erddap/tabledap/OSMC_30day.json"
        "?platform_code,platform_type,time,latitude,longitude"
        "&time%3E={since}&latitude%3E={lat0}&latitude%3C={lat1}"
        "&longitude%3E={lon0}&longitude%3C={lon1}")
GDP = ("https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/drifter_6hour_qc.json"
       "?ID,time,latitude,longitude,ve,vn"
       "&time%3E={since}&latitude%3E={lat0}&latitude%3C={lat1}"
       "&longitude%3E={lon0}&longitude%3C={lon1}")

LIGHT = dict(surface="#fcfcfb", page="#f9f9f7", ink="#0b0b0b", ink2="#52514e",
             muted="#898781", grid="#e1e0d9", axis="#c3c2b7",
             s1="#2a78d6", s2="#eb6834", land="#f1f0ea", sea="#fcfcfb")
DARK = dict(surface="#1a1a19", page="#0d0d0d", ink="#ffffff", ink2="#c3c2b7",
            muted="#898781", grid="#2c2c2a", axis="#383835",
            s1="#3987e5", s2="#d95926", land="#232322", sea="#141413")


def erddap(url, name, max_age_h=3):
    """Fetch an ERDDAP json table into a DataFrame, cached on disk."""
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / name
    fresh = (path.exists()
             and dt.datetime.now().timestamp() - path.stat().st_mtime < max_age_h * 3600)
    if not fresh:
        print(f"fetching {name} ...")
        try:
            urllib.request.urlretrieve(url, path)
        except Exception as exc:                       # keep a stale copy usable
            if not path.exists():
                raise
            print(f"  fetch failed ({exc}); using cached copy")
    table = json.loads(path.read_text())["table"]
    df = pd.DataFrame(table["rows"], columns=table["columnNames"])
    df["time"] = pd.to_datetime(df["time"])
    for c in ("latitude", "longitude", "ve", "vn"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ----------------------------------------------------------------------
# 1. DATA
# ----------------------------------------------------------------------

since = (dt.datetime.now(dt.UTC) - dt.timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
live = erddap(OSMC.format(since=since, **BOX), "osmc_30day.json")
live = live[live["platform_type"].str.contains("DRIFT", case=False, na=False)]
live = live.drop_duplicates(["platform_code", "time"]).sort_values("time")

hist = erddap(GDP.format(since=HIST_FROM, **CORRIDOR), "gdp_corridor.json", max_age_h=24 * 30)
hist = hist[(hist["ve"].abs() < 300) & (hist["vn"].abs() < 300)].copy()
hist["speed_cm_s"] = np.hypot(hist["ve"], hist["vn"]) * 100

ashk_lat, ashk_lon = POINTS["Ashkelon_IL"]

MAX_DRIFT_M_S = 2.0     # no surface drifter here does 2 m/s; faster => bad fix
STATIONARY_KM = 5.0     # moved less than this in 30 days => beached or moored


def km_to(lat, lon, lat0=ashk_lat, lon0=ashk_lon):
    return np.hypot((lat - lat0) * 111.19,
                    (lon - lon0) * 111.19 * np.cos(np.radians(lat0)))


def despike(g):
    """Drop position reports that imply an impossible speed from the last good
    fix. The raw OSMC feed carries occasional wild outliers - one bad fix is
    enough to draw a 200 km leg that never happened."""
    keep, last = [], None
    for row in g.sort_values("time").itertuples():
        if last is not None:
            hours = (row.time - last.time).total_seconds() / 3600
            if hours <= 0:
                continue
            km = km_to(row.latitude, row.longitude, last.latitude, last.longitude)
            if km / hours > MAX_DRIFT_M_S * 3.6:
                continue
        keep.append(row.Index)
        last = row
    return g.loc[keep]


# ----------------------------------------------------------------------
# 2. MAP
# ----------------------------------------------------------------------

fig = go.Figure()

# Context first, so live tracks draw on top.
fig.add_trace(go.Scattergeo(
    lat=hist["latitude"], lon=hist["longitude"], mode="markers",
    name=f"drifter fixes 2015-{hist['time'].max():%Y} ({hist['ID'].nunique()} drifters)",
    marker=dict(size=4, color=LIGHT["muted"], opacity=0.45),
    customdata=np.column_stack([hist["ID"], hist["time"].dt.strftime("%d %b %Y %H:%MZ"),
                                hist["speed_cm_s"].round(1)]),
    hovertemplate=("drifter %{customdata[0]}<br>%{customdata[1]}"
                   "<br>%{lat:.2f}°N %{lon:.2f}°E<br>speed %{customdata[2]:.0f} cm/s"
                   "<extra></extra>"),
))

fig.add_trace(go.Scattergeo(
    lat=[POINTS[s][0] for s in POINTS], lon=[POINTS[s][1] for s in POINTS],
    mode="markers", name="our model stations",
    marker=dict(size=7, color=LIGHT["ink"], symbol="diamond",
                line=dict(width=1.5, color=LIGHT["surface"])),
    text=[s.replace("_", " ") for s in POINTS],
    hovertemplate="%{text}<br>%{lat:.2f}°N %{lon:.2f}°E<extra></extra>",
))

live_traces, labels, status = [], [], {}
for i, (code, g) in enumerate(live.groupby("platform_code")):
    colour = (LIGHT["s1"], LIGHT["s2"])[i % 2]
    dropped = len(g)
    g = despike(g)
    dropped -= len(g)
    moved = km_to(g["latitude"].iloc[-1], g["longitude"].iloc[-1],
                  g["latitude"].iloc[0], g["longitude"].iloc[0])
    # Spread of the position cloud, not the summed path: a buoy sitting still
    # still logs GPS jitter, and 30 days of jitter sums to tens of kilometres.
    spread = km_to(g["latitude"], g["longitude"],
                   g["latitude"].median(), g["longitude"].median()).max()
    still = spread < STATIONARY_KM
    status[code] = dict(moved=moved, spread=spread, still=still, dropped=dropped,
                        last=g["time"].iloc[-1], lat=g["latitude"].iloc[-1],
                        lon=g["longitude"].iloc[-1])
    label = (f"{code} · stationary (beached or moored)" if still
             else f"{code} · {moved:.0f} km in 30 d")
    fig.add_trace(go.Scattergeo(
        lat=g["latitude"], lon=g["longitude"], mode="lines",
        name=label, line=dict(width=2, color=colour),
        hovertemplate=f"drifter {code}<br>%{{lat:.2f}}°N %{{lon:.2f}}°E<extra></extra>",
    ))
    live_traces.append(len(fig.data) - 1)
    fig.add_trace(go.Scattergeo(          # current position, ringed end-dot
        lat=[g["latitude"].iloc[-1]], lon=[g["longitude"].iloc[-1]], mode="markers",
        showlegend=False, name=code,
        marker=dict(size=13, color=colour, line=dict(width=2, color=LIGHT["surface"])),
        customdata=[[code, g["time"].iloc[-1].strftime("%d %b %Y %H:%MZ"),
                     km_to(g["latitude"].iloc[-1], g["longitude"].iloc[-1])]],
        hovertemplate=("<b>drifter %{customdata[0]}</b><br>last fix %{customdata[1]}"
                       "<br>%{lat:.2f}°N %{lon:.2f}°E"
                       "<br>%{customdata[2]:.0f} km from Ashkelon<extra></extra>"),
    ))
    live_traces.append(len(fig.data) - 1)
    labels.append(dict(lat=float(g["latitude"].iloc[-1]), lon=float(g["longitude"].iloc[-1]),
                       text=f"  {code}", colour=colour))

fig.update_layout(
    geo=dict(scope="world", resolution=50, projection_type="mercator",
             lataxis=dict(range=[BOX["lat0"], BOX["lat1"]]),
             lonaxis=dict(range=[BOX["lon0"], BOX["lon1"]]),
             showland=True, landcolor=LIGHT["land"], showocean=True,
             oceancolor=LIGHT["sea"], showcoastlines=True,
             coastlinecolor=LIGHT["axis"], coastlinewidth=1,
             showcountries=True, countrycolor=LIGHT["grid"], countrywidth=1,
             showframe=False, bgcolor=LIGHT["surface"]),
    height=850, margin=dict(l=0, r=0, t=0, b=0),
    legend=dict(orientation="h", yanchor="bottom", y=0.005, xanchor="left", x=0.005,
                bgcolor="rgba(0,0,0,0)", font=dict(size=11.5)),
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', size=12),
    dragmode="pan",
)

div = fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="map",
                  config={"displayModeBar": False, "responsive": True, "scrollZoom": True})

# ----------------------------------------------------------------------
# 3. TABLE VIEW + PAGE
# ----------------------------------------------------------------------

rows = ""
for code, s in status.items():
    rows += (f"<tr><th>{code}</th>"
             f"<td>{s['last']:%d %b %Y %H:%MZ}</td>"
             f"<td>{s['lat']:.2f}</td><td>{s['lon']:.2f}</td>"
             f"<td>{km_to(s['lat'], s['lon']):,.0f}</td>"
             f"<td>{s['moved']:,.0f}</td>"
             f"<td style='text-align:left'>"
             f"{'stationary - beached or moored' if s['still'] else 'drifting'}</td></tr>")

near50 = int((km_to(hist["latitude"], hist["longitude"]) < 50).sum())
days50 = hist[km_to(hist["latitude"], hist["longitude"]) < 50]["time"].dt.date.nunique()

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Surface drifters near Israel</title>
<style>
  :root {{ color-scheme: light; }}
  body {{ margin:0; padding:28px 24px 56px; background:var(--page); color:var(--ink);
    font:14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }}
  .viz-root {{
    --surface:{LIGHT['surface']}; --page:{LIGHT['page']}; --ink:{LIGHT['ink']};
    --ink2:{LIGHT['ink2']}; --muted:{LIGHT['muted']}; --axis:{LIGHT['axis']};
    --border:rgba(11,11,11,0.10); }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) .viz-root {{ color-scheme: dark;
      --surface:{DARK['surface']}; --page:{DARK['page']}; --ink:{DARK['ink']};
      --ink2:{DARK['ink2']}; --muted:{DARK['muted']}; --axis:{DARK['axis']};
      --border:rgba(255,255,255,0.10); }} }}
  :root[data-theme="dark"] .viz-root {{ color-scheme: dark;
    --surface:{DARK['surface']}; --page:{DARK['page']}; --ink:{DARK['ink']};
    --ink2:{DARK['ink2']}; --muted:{DARK['muted']}; --axis:{DARK['axis']};
    --border:rgba(255,255,255,0.10); }}
  .wrap {{ max-width:1100px; margin:0 auto; }}
  h1 {{ font-size:19px; font-weight:600; margin:0 0 4px; letter-spacing:-0.01em; }}
  .sub {{ color:var(--ink2); margin:0 0 2px; }}
  .meta {{ color:var(--muted); font-size:12.5px; margin:0 0 16px; }}
  .bar {{ display:flex; gap:18px; margin:0 0 12px; align-items:center; }}
  .spacer {{ flex:1 }}
  button {{ font:inherit; font-size:12.5px; color:var(--ink2); background:var(--surface);
    border:1px solid var(--border); border-radius:7px; padding:5px 11px; cursor:pointer; }}
  button:hover {{ color:var(--ink); }}
  .card {{ background:var(--surface); border:1px solid var(--border);
    border-radius:10px; overflow:hidden; }}
  details {{ margin-top:22px; }}
  summary {{ cursor:pointer; color:var(--ink2); font-size:13px; }}
  table {{ border-collapse:collapse; width:100%; font-size:12.5px; margin-top:12px;
    font-variant-numeric:tabular-nums; background:var(--surface);
    border:1px solid var(--border); border-radius:10px; }}
  th,td {{ padding:6px 10px; text-align:right; white-space:nowrap; }}
  thead th {{ color:var(--ink2); font-weight:600; border-bottom:1px solid var(--axis); }}
  tbody th {{ text-align:left; font-weight:400; color:var(--ink2); }}
</style>
</head>
<body class="viz-root">
<div class="wrap">
  <h1>Surface drifters in the eastern Mediterranean</h1>
  <p class="sub">Real GPS-tracked buoys &mdash; actual measurements, not model output.
     Coloured lines are the last 30 days of each live drifter, the ringed dot its latest fix.
     Grey dots are every quality-controlled drifter fix in the corridor since 2015.
     Live tracks are despiked: position reports implying more than 2 m/s are dropped.</p>
  <p class="meta">Live positions from NOAA OSMC &middot; historical fixes from the NOAA AOML
     Global Drifter Program &middot; {len(live.platform_code.unique())} live drifter(s) in view,
     {hist['ID'].nunique()} historical drifters, {near50:,} fixes within 50 km of Ashkelon
     across {days50} days.</p>
  <div class="bar"><span class="spacer"></span><button id="theme">Dark mode</button></div>
  <div class="card">{div}</div>

  <details open>
    <summary>Live drifters &mdash; table view</summary>
    <table>
      <thead><tr><th style="text-align:left">Platform</th><th>Last fix</th><th>Lat °N</th>
        <th>Lon °E</th><th>km from Ashkelon</th><th>km moved in 30 d</th>
        <th style="text-align:left">Status</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </details>
</div>
<script>
const LIGHT={json.dumps(LIGHT)}, DARK={json.dumps(DARK)};
const LIVE={json.dumps(live_traces)};
const gd=document.getElementById("map");
function applyTheme(dark){{
  const c=dark?DARK:LIGHT;
  Plotly.relayout(gd,{{"paper_bgcolor":c.surface,"plot_bgcolor":c.surface,"font.color":c.ink2,
    "geo.landcolor":c.land,"geo.oceancolor":c.sea,"geo.coastlinecolor":c.axis,
    "geo.countrycolor":c.grid,"geo.bgcolor":c.surface,
    "hoverlabel.bgcolor":c.surface,"hoverlabel.bordercolor":c.axis,"hoverlabel.font.color":c.ink}});
  Plotly.restyle(gd,{{"marker.color":[c.muted]}},[0]);
  Plotly.restyle(gd,{{"marker.color":[c.ink],"marker.line.color":[c.surface]}},[1]);
  LIVE.forEach((t,i)=>{{
    const col=(i%4<2)?c.s1:c.s2;
    Plotly.restyle(gd, gd.data[t].mode==="lines"?{{"line.color":[col]}}
      :{{"marker.color":[col],"marker.line.color":[c.surface]}},[t]);
  }});
}}
const mq=window.matchMedia("(prefers-color-scheme: dark)");
let stamped=null;
const cur=()=>stamped===null?mq.matches:stamped;
function render(){{const d=cur();document.documentElement.dataset.theme=d?"dark":"light";
  document.getElementById("theme").textContent=d?"Light mode":"Dark mode";applyTheme(d);}}
document.getElementById("theme").onclick=()=>{{stamped=!cur();render();}};
mq.addEventListener("change",()=>{{if(stamped===null)render();}});
render();
</script>
</body>
</html>
"""

OUTFILE.write_text(HTML, encoding="utf-8")
print(f"Wrote {OUTFILE.name}: {live['platform_code'].nunique()} live drifter(s), "
      f"{hist['ID'].nunique()} historical drifters, {len(hist):,} fixes.")
for code, g in live.groupby("platform_code"):
    d = km_to(g["latitude"].iloc[-1], g["longitude"].iloc[-1])
    print(f"  {code}: last fix {g['time'].iloc[-1]:%d %b %Y %H:%MZ} at "
          f"{g['latitude'].iloc[-1]:.2f}N {g['longitude'].iloc[-1]:.2f}E - {d:.0f} km from Ashkelon")
