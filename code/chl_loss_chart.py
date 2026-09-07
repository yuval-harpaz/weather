"""
Interactive comparison of the three ocean-colour sensors at each site.

One subplot per site, ordered along the coast from Hadera in the north round to
the Nile in the west. Three traces per subplot, one per sensor. A month toggle
and a metric dropdown drive the whole figure at once.

Metrics include the two kinds of data loss that matter separately:
    station pixel   - is the 4 km pixel itself there on a given day
    whole line      - what fraction of the transect's sea pixels are there
    loss vs shore   - how that fraction varies with distance from the coast,
                      which is where land adjacency shows up

RUN:
    python chl_loss_chart.py

OUTPUT:
    chl_loss.html
"""

import json
import pathlib

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
OUT = HERE / "chl_loss.html"

SENSORS = {"olci300m": "OLCI 300 m", "multi1km": "multi-sensor 1 km",
           "hr100m": "Sentinel-2 100 m"}
MONTHS = {"202608": "August 2026", "202603": "March 2026"}
DEFAULT_MONTH = "202608"

# North, then south along the coast, then west along Sinai and the delta.
ORDER = ["Hadera_IL", "Tel_Aviv_IL", "Ashdod_IL", "Ashkelon_IL", "Gaza_City",
         "Rafah_Gaza", "El_Arish_EG", "Bardawil_off", "Damietta_mouth",
         "Rosetta_mouth", "Open_sea_ref"]

LIGHT = dict(surface="#fcfcfb", page="#f9f9f7", ink="#0b0b0b", ink2="#52514e",
             muted="#898781", grid="#e1e0d9", minorgrid="#efeee9", axis="#c3c2b7",
             s=["#2a78d6", "#eb6834", "#1baf7a"])
DARK = dict(surface="#1a1a19", page="#0d0d0d", ink="#ffffff", ink2="#c3c2b7",
            muted="#898781", grid="#2c2c2a", minorgrid="#232322", axis="#383835",
            s=["#3987e5", "#d95926", "#199e70"])

METRICS = {
    "station_chl":  dict(dtick=1, minor_dtick="D2", label="Chlorophyll at the 4 km station",
                         unit="mg m⁻³", axis="log", x="date"),
    "line_max":     dict(label="Chlorophyll — transect maximum",
                         unit="mg m⁻³", axis="log", x="date"),
    "line_mean":    dict(label="Chlorophyll — transect mean",
                         unit="mg m⁻³", axis="log", x="date"),
    # Station value against the two transect statistics, one sensor at a time -
    # the comparison that says whether a fixed pixel or the whole line carries
    # the clearer signal. Missing days show as gaps, so loss is still visible.
    "cmp_olci300m": dict(label="Station vs transect — OLCI 300 m",
                         unit="mg m⁻³", axis="log", x="date", compare="olci300m"),
    "cmp_multi1km": dict(label="Station vs transect — multi-sensor 1 km",
                         unit="mg m⁻³", axis="log", x="date", compare="multi1km"),
    "cmp_hr100m":   dict(dtick=1, minor_dtick="D2", label="Station vs transect — Sentinel-2 100 m",
                         unit="mg m⁻³", axis="log", x="date", compare="hr100m"),
    "line_loss":    dict(dtick=50, minor_dtick=10, label="Whole line — valid sea pixels per day",
                         unit="%", axis="linear", x="date", range=[-3, 103]),
    "loss_by_dist": dict(dtick=50, minor_dtick=10, label="Loss vs distance from shore (month mean)",
                         unit="% of days present", axis="linear", x="distance",
                         range=[-3, 103]),
    "station_tur":  dict(dtick=1, minor_dtick="D2", label="Turbidity at the 4 km station (100 m only)",
                         unit="FNU", axis="log", x="date"),
}


def load():
    summ, prof = {}, {}
    for sensor in SENSORS:
        for month in MONTHS:
            c = DATA / f"chl_transect_{sensor}_{month}.csv"
            p = DATA / f"chl_profile_{sensor}_{month}.parquet"
            if c.exists():
                d = pd.read_csv(c)
                d["date"] = pd.to_datetime(d["date"])
                summ[(sensor, month)] = d
            if p.exists():
                prof[(sensor, month)] = pd.read_parquet(p)
    return summ, prof


summary, profiles = load()


COMPARE_QUANTITIES = ["at_station", "transect_max", "transect_mean"]
COMPARE_NAMES = ["at the 4 km station", "transect max", "transect mean"]


def series(metric, sensor, month, site, slot=0):
    """(x, y) for one trace, as plain lists ready for JSON.

    `slot` is which of the three traces in a panel this is. Normally that means
    which sensor; in a compare view the sensor is fixed and the slot selects
    station / max / mean instead.
    """
    cfg = METRICS[metric]
    if cfg.get("compare"):
        sensor = cfg["compare"]
        s = summary.get((sensor, month))
        if s is None:
            return [], []
        q = s[(s.site == site) & (s.variable == "CHL")].sort_values("date")
        if q.empty:
            return [], []
        col = COMPARE_QUANTITIES[slot]
        if col not in q or (col != "at_station" and not q.get("transect_max") is not None):
            return [], []
        x = [d.strftime("%Y-%m-%d") for d in q["date"]]
        return x, [None if pd.isna(v) else round(float(v), 4) for v in q[col]]

    s = summary.get((sensor, month))
    if s is None:
        return [], []

    if metric == "loss_by_dist":
        p = profiles.get((sensor, month))
        if p is None:
            return [], []
        q = p[(p.site == site) & (p.variable == "CHL") & p.is_sea]
        if q.empty:
            return [], []
        g = q.groupby("distance_km")["value"].apply(lambda v: 100 * v.notna().mean())
        return [round(float(v), 3) for v in g.index], [round(float(v), 1) for v in g.values]

    var = "TUR" if metric == "station_tur" else "CHL"
    q = s[(s.site == site) & (s.variable == var)].sort_values("date")
    if q.empty:
        return [], []
    x = [d.strftime("%Y-%m-%d") for d in q["date"]]
    if metric == "station_chl" or metric == "station_tur":
        y = q["at_station"]
    elif metric == "line_max":
        y = q["transect_max"]
    elif metric == "line_mean":
        y = q["transect_mean"]
    elif metric == "line_loss":
        y = q["valid_frac"] * 100
    return x, [None if pd.isna(v) else round(float(v), 4) for v in y]


# ----------------------------------------------------------------------
# figure: one row per site, three sensor traces each
# ----------------------------------------------------------------------

fig = make_subplots(rows=len(ORDER), cols=1, shared_xaxes=True,
                    vertical_spacing=0.010)

trace_order = []
for r, site in enumerate(ORDER, start=1):
    for k, sensor in enumerate(SENSORS):
        x, y = series("station_chl", sensor, DEFAULT_MONTH, site)
        fig.add_trace(go.Scatter(
            x=x, y=y, mode="lines+markers", name=SENSORS[sensor],
            legendgroup=sensor, showlegend=(r == 1),
            line=dict(width=2, color=LIGHT["s"][k]),
            marker=dict(size=4.5),
            connectgaps=False,
            hovertemplate="%{x}<br>%{fullData.name}: %{y}<extra></extra>",
        ), row=r, col=1)
        trace_order.append((site, sensor))

ROW_H = 128
fig.update_yaxes(type="log", dtick=1, tickfont=dict(size=10),
                 gridcolor=LIGHT["grid"], gridwidth=1,
                 minor=dict(showgrid=True, dtick="D2",
                            gridcolor=LIGHT["minorgrid"], gridwidth=1))
fig.update_xaxes(tickfont=dict(size=10))
fig.update_layout(
    height=ROW_H * len(ORDER) + 120,
    margin=dict(l=68, r=16, t=8, b=40),
    hovermode="x unified", hoverlabel=dict(font_size=12),
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', size=12),
    legend=dict(orientation="h", yanchor="bottom", y=1.004, xanchor="left", x=0,
                bgcolor="rgba(0,0,0,0)"),
    dragmode="pan",
)
annotations = []
for r, site in enumerate(ORDER, start=1):
    annotations.append(dict(
        xref="x domain", yref=f"y{r if r > 1 else ''} domain",
        x=0.004, y=1.0, xanchor="left", yanchor="top", showarrow=False,
        text=f"<b>{site.replace('_', ' ')}</b>", font=dict(size=11.5)))
fig.update_layout(annotations=annotations)

# ----------------------------------------------------------------------
# precomputed payload for the controls
# ----------------------------------------------------------------------

payload = {}
for metric in METRICS:
    cfg = METRICS[metric]
    payload[metric] = {}
    for month in MONTHS:
        xs, ys = [], []
        for k, (site, sensor) in enumerate(trace_order):
            x, y = series(metric, sensor, month, site, slot=k % len(SENSORS))
            xs.append(x)
            ys.append(y)
        payload[metric][month] = dict(x=xs, y=ys)
    payload[metric]["names"] = (COMPARE_NAMES if cfg.get("compare")
                                else list(SENSORS.values()))

    # One y range per metric, shared by every panel and by both months, so the
    # sites are directly comparable and switching month does not rescale.
    if "range" not in cfg:
        vals = [v for m in MONTHS for ys in payload[metric][m]["y"]
                for v in ys if v is not None and np.isfinite(v)]
        vals = [v for v in vals if v > 0] if cfg["axis"] == "log" else vals
        if vals:
            lo, hi = float(min(vals)), float(max(vals))
            if cfg["axis"] == "log":
                cfg["range"] = [np.log10(lo) - 0.08, np.log10(hi) + 0.08]
            else:
                pad = 0.05 * (hi - lo or 1.0)
                cfg["range"] = [lo - pad, hi + pad]
            cfg["range"] = [round(float(v), 4) for v in cfg["range"]]

fig.update_yaxes(range=METRICS["station_chl"]["range"], autorange=False)
div = fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="chart",
                  config={"displayModeBar": False, "responsive": True})

options = "".join(f'<option value="{k}">{v["label"]}</option>'
                  for k, v in METRICS.items())
month_buttons = "".join(
    f'<button class="month{" on" if k == DEFAULT_MONTH else ""}" data-month="{k}">{v}</button>'
    for k, v in MONTHS.items())

HTML = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ocean-colour sensor comparison — Nile to Hadera</title>
<style>
  :root {{ color-scheme: light; }}
  body {{ margin:0; padding:26px 22px 56px; background:var(--page); color:var(--ink);
    font:14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }}
  .viz-root {{
    --surface:{LIGHT['surface']}; --page:{LIGHT['page']}; --ink:{LIGHT['ink']};
    --ink2:{LIGHT['ink2']}; --muted:{LIGHT['muted']}; --grid:{LIGHT['grid']};
    --axis:{LIGHT['axis']}; --border:rgba(11,11,11,0.10); }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) .viz-root {{ color-scheme: dark;
      --surface:{DARK['surface']}; --page:{DARK['page']}; --ink:{DARK['ink']};
      --ink2:{DARK['ink2']}; --muted:{DARK['muted']}; --grid:{DARK['grid']};
      --axis:{DARK['axis']}; --border:rgba(255,255,255,0.10); }} }}
  :root[data-theme="dark"] .viz-root {{ color-scheme: dark;
    --surface:{DARK['surface']}; --page:{DARK['page']}; --ink:{DARK['ink']};
    --ink2:{DARK['ink2']}; --muted:{DARK['muted']}; --grid:{DARK['grid']};
    --axis:{DARK['axis']}; --border:rgba(255,255,255,0.10); }}
  .wrap {{ max-width:1180px; margin:0 auto; }}
  h1 {{ font-size:19px; font-weight:600; margin:0 0 4px; letter-spacing:-0.01em; }}
  .sub {{ color:var(--ink2); margin:0 0 14px; }}
  .bar {{ display:flex; gap:10px; align-items:center; margin:0 0 14px; flex-wrap:wrap; }}
  .spacer {{ flex:1 }}
  label {{ color:var(--muted); font-size:12.5px; }}
  select, button {{ font:inherit; font-size:12.5px; color:var(--ink2);
    background:var(--surface); border:1px solid var(--border); border-radius:7px;
    padding:6px 11px; cursor:pointer; }}
  select {{ min-width:340px; }}
  button.on {{ color:var(--ink); border-color:var(--axis); font-weight:600; }}
  button:hover, select:hover {{ color:var(--ink); }}
  .card {{ background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:10px 4px 4px; }}
  .note {{ color:var(--muted); font-size:12.5px; margin:10px 2px 0; }}
</style></head>
<body class="viz-root">
<div class="wrap">
  <h1>Ocean-colour sensors compared, site by site</h1>
  <p class="sub">Sites ordered along the coast: Hadera at the top, south past Gaza and Sinai,
     then west to the Nile mouths. Each panel shows all three sensors for the selected month.</p>
  <div class="bar">
    <label for="metric">Data</label>
    <select id="metric">{options}</select>
    <span class="spacer"></span>
    {month_buttons}
    <button id="theme">Dark mode</button>
  </div>
  <div class="card">{div}</div>
  <p class="note" id="note"></p>
</div>
<script>
const PAYLOAD = {json.dumps(payload)};
const METRICS = {json.dumps(METRICS)};
const LIGHT = {json.dumps(LIGHT)}, DARK = {json.dumps(DARK)};
const NROWS = {len(ORDER)};
const NSENS = {len(SENSORS)};
const gd = document.getElementById("chart");
let metric = "station_chl", month = "{DEFAULT_MONTH}";

function apply() {{
  const p = PAYLOAD[metric][month], cfg = METRICS[metric];
  const names = [];
  for (let r = 0; r < NROWS; r++) for (let k = 0; k < NSENS; k++)
    names.push(PAYLOAD[metric].names[k]);
  Plotly.restyle(gd, {{x: p.x, y: p.y, name: names}});
  const lay = {{}};
  for (let r = 1; r <= NROWS; r++) {{
    const y = r === 1 ? "yaxis" : "yaxis" + r;
    lay[y + ".type"] = cfg.axis;
    lay[y + ".dtick"] = cfg.dtick;
    lay[y + ".minor.dtick"] = cfg.minor_dtick;
    lay[y + ".minor.showgrid"] = true;
    lay[y + ".autorange"] = cfg.range ? false : true;
    if (cfg.range) lay[y + ".range"] = cfg.range;
    const x = r === 1 ? "xaxis" : "xaxis" + r;
    lay[x + ".title"] = (r === NROWS && cfg.x === "distance")
      ? "distance from shore (km)" : "";
  }}
  Plotly.relayout(gd, lay);
  document.getElementById("note").textContent =
    cfg.label + " · " + cfg.unit + (cfg.x === "distance"
      ? " · x axis is distance from the shore, averaged over the month"
      : " · x axis is date");
}}

document.getElementById("metric").onchange = e => {{ metric = e.target.value; apply(); }};
document.querySelectorAll("button.month").forEach(b => b.onclick = () => {{
  month = b.dataset.month;
  document.querySelectorAll("button.month").forEach(o => o.classList.toggle("on", o === b));
  apply();
}});

function applyTheme(dark) {{
  const c = dark ? DARK : LIGHT;
  const lay = {{"paper_bgcolor": c.surface, "plot_bgcolor": c.surface,
    "font.color": c.ink2, "hoverlabel.bgcolor": c.surface,
    "hoverlabel.bordercolor": c.axis, "hoverlabel.font.color": c.ink}};
  for (let r = 1; r <= NROWS; r++) {{
    const x = r === 1 ? "xaxis" : "xaxis" + r, y = r === 1 ? "yaxis" : "yaxis" + r;
    lay[x + ".gridcolor"] = c.grid; lay[x + ".linecolor"] = c.axis;
    lay[x + ".tickfont.color"] = c.muted; lay[x + ".zerolinecolor"] = c.axis;
    lay[y + ".gridcolor"] = c.grid; lay[y + ".linecolor"] = c.axis;
    lay[y + ".minor.gridcolor"] = c.minorgrid; lay[y + ".minor.gridwidth"] = 1;
    lay[y + ".tickfont.color"] = c.muted; lay[y + ".zerolinecolor"] = c.axis;
  }}
  lay["annotations"] = gd.layout.annotations.map(a =>
    Object.assign({{}}, a, {{font: Object.assign({{}}, a.font, {{color: c.ink}})}}));
  Plotly.relayout(gd, lay);
  const colours = [];
  for (let r = 0; r < NROWS; r++) for (let k = 0; k < NSENS; k++) colours.push(c.s[k]);
  Plotly.restyle(gd, {{"line.color": colours, "marker.color": colours}});
}}
const mq = window.matchMedia("(prefers-color-scheme: dark)");
let stamped = null;
const cur = () => stamped === null ? mq.matches : stamped;
function render() {{ const d = cur();
  document.documentElement.dataset.theme = d ? "dark" : "light";
  document.getElementById("theme").textContent = d ? "Light mode" : "Dark mode";
  applyTheme(d); }}
document.getElementById("theme").onclick = () => {{ stamped = !cur(); render(); }};
mq.addEventListener("change", () => {{ if (stamped === null) render(); }});
render(); apply();
</script>
</body></html>
"""

OUT.write_text(HTML, encoding="utf-8")
print(f"wrote {OUT.name} ({OUT.stat().st_size / 1e6:.1f} MB)")

# quick text summary of the two loss metrics
print("\nstation-pixel presence vs whole-line presence, % of days (CHL)")
for month in MONTHS:
    print(f"\n{MONTHS[month]}")
    print(f"{'site':<16}" + "".join(f"{SENSORS[s][:9]:>22}" for s in SENSORS))
    print(f"{'':<16}" + "".join(f"{'station':>11}{'line':>11}" for _ in SENSORS))
    for site in ORDER:
        cells = ""
        for sensor in SENSORS:
            s = summary.get((sensor, month))
            q = s[(s.site == site) & (s.variable == "CHL")] if s is not None else None
            if q is None or q.empty:
                cells += f"{'-':>11}{'-':>11}"
            else:
                cells += (f"{100 * q.at_station.notna().mean():11.0f}"
                          f"{100 * q.valid_frac.mean():11.0f}")
        print(f"{site:<16}{cells}")
