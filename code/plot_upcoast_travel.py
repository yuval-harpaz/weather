"""
Build a Plotly HTML chart of daily up-coast travel from the Excel file written
by get_currents_excel.py. Reads the workbook only - it does not touch CMEMS.

Up-coast travel is the daily displacement along each station's OWN shore angle
(see shore_angle.py), so Sinai's ENE-running coast is not measured on Israel's
NNE axis.

RUN:
    python plot_upcoast_travel.py [israel_currents_hourly.xlsx]

OUTPUT:
    upcoast_travel.html   - open in a browser, no server needed
"""

import sys
import json
import pathlib

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from coast_points import POINTS, SHORE_AZIMUTH_DEG

INFILE = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "israel_currents_hourly.xlsx")
OUTFILE = INFILE.with_name("upcoast_travel.html")

# Palette (see dataviz reference): diverging blue <-> red, neutral gray midpoint.
LIGHT = dict(
    surface="#fcfcfb", page="#f9f9f7", ink="#0b0b0b", ink2="#52514e", muted="#898781",
    grid="#e1e0d9", axis="#c3c2b7", pos="#2a78d6", neg="#e34948",
)
DARK = dict(
    surface="#1a1a19", page="#0d0d0d", ink="#ffffff", ink2="#c3c2b7", muted="#898781",
    grid="#2c2c2a", axis="#383835", pos="#3987e5", neg="#e66767",
)

# ----------------------------------------------------------------------
# 1. LOAD
# ----------------------------------------------------------------------

daily = pd.read_excel(INFILE, sheet_name="daily")
daily["date"] = pd.to_datetime(daily["date"])

# Drop partial days: the last date carries a single 00:00 sample when END is a
# midnight boundary, and one hour extrapolated over 86400 s is not a daily mean.
dropped = daily[daily["n_hours"] < 24][["station", "date"]]
daily = daily[daily["n_hours"] == 24].copy()

# North -> south, so the northmost station is the top subplot.
stations = sorted(daily["station"].unique(), key=lambda s: POINTS[s][0], reverse=True)

# ----------------------------------------------------------------------
# 2. FIGURE - one facet per station, shared x and y
# ----------------------------------------------------------------------

ROW_H = 132
fig = make_subplots(rows=len(stations), cols=1, shared_xaxes=True, vertical_spacing=0.012)

vmax = daily["upcoast_travel_km"].max()
vmin = daily["upcoast_travel_km"].min()
pad = (vmax - vmin) * 0.12
yrange = [vmin - pad, vmax + pad]

colors_light, colors_dark, annotations = [], [], []

for i, st in enumerate(stations, start=1):
    d = daily[daily["station"] == st].sort_values("date")
    sign_pos = d["upcoast_travel_km"] >= 0
    colors_light.append([LIGHT["pos"] if p else LIGHT["neg"] for p in sign_pos])
    colors_dark.append([DARK["pos"] if p else DARK["neg"] for p in sign_pos])

    fig.add_trace(
        go.Bar(
            x=d["date"], y=d["upcoast_travel_km"],
            marker=dict(color=colors_light[-1], line=dict(width=0)),
            name=st, showlegend=False,
            customdata=d[["mean_upcoast_cm_s", "mean_speed_cm_s",
                          "mean_crossshore_cm_s"]].round(2).values,
            hovertemplate=(
                "<b>%{x|%a %d %b %Y}</b><br>"
                + st.replace("_", " ") + "<br>"
                "Up-coast travel: %{y:.1f} km<br>"
                "Mean up-coast: %{customdata[0]:.1f} cm/s<br>"
                "Mean cross-shore: %{customdata[2]:.1f} cm/s (+ = offshore)<br>"
                "Mean speed: %{customdata[1]:.1f} cm/s<extra></extra>"
            ),
        ),
        row=i, col=1,
    )

    net = d["upcoast_travel_km"].sum()
    back_days = int((d["upcoast_travel_km"] < 0).sum())
    # Facet identity, top-left inside the panel; summary stat top-right.
    annotations.append(dict(
        xref="x domain", yref=f"y{i if i > 1 else ''} domain", x=0.005, y=1.0,
        xanchor="left", yanchor="top", showarrow=False,
        text=(f"<b>{st.replace('_', ' ')}</b>  {POINTS[st][0]:.2f}°N"
              f" · shore {SHORE_AZIMUTH_DEG[st]:.0f}°"),
        font=dict(size=12), align="left",
    ))
    annotations.append(dict(
        xref="x domain", yref=f"y{i if i > 1 else ''} domain", x=0.995, y=1.0,
        xanchor="right", yanchor="top", showarrow=False,
        text=f"net {net:+,.0f} km · {back_days} down-coast days",
        font=dict(size=11), align="right", name=f"sub{i}",
    ))

fig.update_traces(marker_line_width=0)
fig.update_yaxes(range=yrange, zeroline=True, zerolinewidth=1, dtick=10,
                 tickfont=dict(size=11))
fig.update_xaxes(showspikes=True, spikemode="across", spikethickness=1, spikedash="solid")
fig.update_layout(
    barmode="relative", bargap=0.15,
    height=ROW_H * len(stations) + 90,
    margin=dict(l=64, r=18, t=8, b=44),
    hovermode="x unified", hoverlabel=dict(font_size=12),
    annotations=annotations,
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', size=12),
    dragmode="pan",
)

div = fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="chart",
                  config={"displayModeBar": False, "responsive": True, "scrollZoom": True})

# ----------------------------------------------------------------------
# 3. TABLE VIEW - the WCAG-clean twin, every plotted value reachable
# ----------------------------------------------------------------------

pivot = (
    daily.pivot(index="date", columns="station", values="upcoast_travel_km")
    [stations].round(2)
)
head = "".join(f"<th>{s.replace('_', ' ')}</th>" for s in stations)
rows = "".join(
    "<tr><th>" + d.strftime("%Y-%m-%d") + "</th>"
    + "".join(f"<td>{v:+.2f}</td>" for v in r) + "</tr>"
    for d, r in pivot.iterrows()
)

span = f"{daily['date'].min():%d %b %Y} - {daily['date'].max():%d %b %Y}"
note = ""
if not dropped.empty:
    skipped = ", ".join(sorted(dropped["date"].dt.strftime("%d %b %Y").unique()))
    note = f" Partial day{'s' if dropped['date'].nunique() > 1 else ''} excluded: {skipped}."

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Daily up-coast travel - SE Mediterranean surface currents</title>
<style>
  :root {{ color-scheme: light; }}
  body {{
    margin: 0; padding: 28px 24px 56px;
    background: var(--page); color: var(--ink);
    font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  }}
  .viz-root {{
    --surface: {LIGHT['surface']}; --page: {LIGHT['page']}; --ink: {LIGHT['ink']};
    --ink2: {LIGHT['ink2']}; --muted: {LIGHT['muted']}; --grid: {LIGHT['grid']};
    --axis: {LIGHT['axis']}; --pos: {LIGHT['pos']}; --neg: {LIGHT['neg']};
    --border: rgba(11,11,11,0.10);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) .viz-root {{
      color-scheme: dark;
      --surface: {DARK['surface']}; --page: {DARK['page']}; --ink: {DARK['ink']};
      --ink2: {DARK['ink2']}; --muted: {DARK['muted']}; --grid: {DARK['grid']};
      --axis: {DARK['axis']}; --pos: {DARK['pos']}; --neg: {DARK['neg']};
      --border: rgba(255,255,255,0.10);
    }}
  }}
  :root[data-theme="dark"] .viz-root {{
    color-scheme: dark;
    --surface: {DARK['surface']}; --page: {DARK['page']}; --ink: {DARK['ink']};
    --ink2: {DARK['ink2']}; --muted: {DARK['muted']}; --grid: {DARK['grid']};
    --axis: {DARK['axis']}; --pos: {DARK['pos']}; --neg: {DARK['neg']};
    --border: rgba(255,255,255,0.10);
  }}
  .wrap {{ max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 19px; font-weight: 600; margin: 0 0 4px; letter-spacing: -0.01em; }}
  .sub {{ color: var(--ink2); margin: 0 0 2px; }}
  .meta {{ color: var(--muted); font-size: 12.5px; margin: 0 0 18px; }}
  .bar {{ display: flex; align-items: center; gap: 18px; margin: 0 0 12px; flex-wrap: wrap; }}
  .key {{ display: flex; align-items: center; gap: 7px; color: var(--ink2); font-size: 12.5px; }}
  .swatch {{ width: 11px; height: 11px; border-radius: 2px; display: inline-block; }}
  .spacer {{ flex: 1 }}
  button {{
    font: inherit; font-size: 12.5px; color: var(--ink2); background: var(--surface);
    border: 1px solid var(--border); border-radius: 7px; padding: 5px 11px; cursor: pointer;
  }}
  button:hover {{ color: var(--ink); }}
  .card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 8px 4px 4px;
  }}
  .ylab {{
    color: var(--muted); font-size: 12px; margin: 0 0 -2px 6px;
  }}
  details {{ margin-top: 22px; }}
  summary {{ cursor: pointer; color: var(--ink2); font-size: 13px; }}
  .tablewrap {{ max-height: 460px; overflow: auto; margin-top: 12px;
    border: 1px solid var(--border); border-radius: 10px; background: var(--surface); }}
  table {{ border-collapse: collapse; width: 100%; font-size: 12.5px;
    font-variant-numeric: tabular-nums; }}
  th, td {{ padding: 5px 10px; text-align: right; white-space: nowrap; }}
  thead th {{ position: sticky; top: 0; background: var(--surface); color: var(--ink2);
    font-weight: 600; border-bottom: 1px solid var(--axis); }}
  tbody th {{ text-align: left; font-weight: 400; color: var(--ink2); }}
  tbody tr:nth-child(even) {{ background: color-mix(in srgb, var(--ink) 3%, transparent); }}
</style>
</head>
<body class="viz-root">
<div class="wrap">
  <h1>Daily up-coast travel along the SE Mediterranean coast</h1>
  <p class="sub">Displacement of a passive surface particle along the shore, km per day, measured on
     each station&rsquo;s own coast angle. Positive = up-coast (Egypt &rarr; Gaza &rarr; Israel).
     Stations ordered north (top) to south (bottom).</p>
  <p class="meta">{span} &middot; CMEMS Mediterranean Sea Physics Reanalysis
     (model output, not measurements), hourly surface currents at 1/24&deg;, daily means.{note}</p>

  <div class="bar">
    <span class="key"><span class="swatch" style="background:var(--pos)"></span> up-coast</span>
    <span class="key"><span class="swatch" style="background:var(--neg)"></span> down-coast</span>
    <span class="spacer"></span>
    <button id="theme">Dark mode</button>
  </div>

  <p class="ylab">km per day</p>
  <div class="card">{div}</div>

  <details>
    <summary>Table view &mdash; every plotted value, km per day</summary>
    <div class="tablewrap">
      <table>
        <thead><tr><th style="text-align:left">Date</th>{head}</tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  </details>
</div>

<script>
const LIGHT = {json.dumps(LIGHT)};
const DARK  = {json.dumps(DARK)};
const BAR_LIGHT = {json.dumps(colors_light)};
const BAR_DARK  = {json.dumps(colors_dark)};
const N = {len(stations)};
const gd = document.getElementById("chart");

function applyTheme(dark) {{
  const c = dark ? DARK : LIGHT;
  const bars = dark ? BAR_DARK : BAR_LIGHT;
  const lay = {{
    "paper_bgcolor": c.surface, "plot_bgcolor": c.surface,
    "font.color": c.ink2, "hoverlabel.bgcolor": c.surface,
    "hoverlabel.bordercolor": c.axis, "hoverlabel.font.color": c.ink,
  }};
  for (let i = 1; i <= N; i++) {{
    const x = i === 1 ? "xaxis" : "xaxis" + i, y = i === 1 ? "yaxis" : "yaxis" + i;
    Object.assign(lay, {{
      [x + ".gridcolor"]: c.grid, [x + ".linecolor"]: c.axis,
      [x + ".tickcolor"]: c.axis, [x + ".tickfont.color"]: c.muted,
      [x + ".spikecolor"]: c.axis,
      [y + ".gridcolor"]: c.grid, [y + ".zerolinecolor"]: c.axis,
      [y + ".tickfont.color"]: c.muted, [y + ".linecolor"]: c.axis,
    }});
  }}
  const ann = gd.layout.annotations.map((a, k) =>
    Object.assign({{}}, a, {{font: Object.assign({{}}, a.font, {{color: k % 2 ? c.muted : c.ink}})}}));
  lay["annotations"] = ann;
  Plotly.relayout(gd, lay);
  Plotly.restyle(gd, {{"marker.color": bars}});
}}

const mq = window.matchMedia("(prefers-color-scheme: dark)");
let stamped = null;
function current() {{ return stamped === null ? mq.matches : stamped; }}
function render() {{
  const dark = current();
  document.documentElement.dataset.theme = dark ? "dark" : "light";
  document.getElementById("theme").textContent = dark ? "Light mode" : "Dark mode";
  applyTheme(dark);
}}
document.getElementById("theme").onclick = () => {{ stamped = !current(); render(); }};
mq.addEventListener("change", () => {{ if (stamped === null) render(); }});
render();
</script>
</body>
</html>
"""

OUTFILE.write_text(HTML, encoding="utf-8")
print(f"Wrote {OUTFILE}: {len(stations)} stations, {pivot.shape[0]} days.")
if not dropped.empty:
    print(f"Excluded partial day(s): {sorted(dropped['date'].dt.date.unique())}")
