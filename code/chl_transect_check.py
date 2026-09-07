"""
Per-site validation plot: where each station sits, where its shore-normal
transect runs, and exactly which pixels get sampled.

One panel per site, zoomed enough that individual satellite pixels are visible.
White line = the transect, small squares = the pixel centres it samples, orange
dot = the station at CHL_OFFSHORE_KM, red line = the OSM coastline.

RUN:
    python chl_transect_check.py [sensor]

    sensor: olci300m (default) | multi1km | hr100m
"""

import json
import pathlib
import sys

import numpy as np
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from coast_points import CHL_OFFSHORE_KM

SENSORS = {
    "olci300m": dict(file="/tmp/oc_300m.nc", var="CHL", label="OLCI 300 m",
                     date="2026-08-29"),
    "multi1km": dict(file="/tmp/oc_1km.nc", var="CHL", label="multi-sensor 1 km",
                     date="2026-08-29"),
    "hr100m":   dict(file="/tmp/oc_100m_sep01.nc", var="CHL",
                     label="Sentinel-2 100 m", date="2026-09-01"),
}
SENSOR = sys.argv[1] if len(sys.argv) > 1 else "olci300m"
CFG = SENSORS[SENSOR]

TRANSECT_KM = 20.0
STEP_KM = 0.1              # sampling step along the line
KM_PER_DEG = 111.19
VMIN, VMAX = 0.03, 30.0
INK, MUTED, GRID = "#0b0b0b", "#898781", "#e1e0d9"

HERE = pathlib.Path(__file__).parent
CHL = json.loads((HERE / "chl_points.json").read_text())
OUT = HERE / f"chl_transect_check_{SENSOR}.png"

ds = xr.open_dataset(CFG["file"])
field = ds[CFG["var"]].squeeze().values
lons, lats = ds.longitude.values, ds.latitude.values

osm = json.loads((pathlib.Path.home() / ".cache/weather/osm_coastline.json").read_text())
COAST = [np.array([[p["lon"], p["lat"]] for p in w["geometry"]])
         for w in osm["elements"] if w.get("geometry")]


def transect_points(site):
    """Points every STEP_KM from the shore anchor out to TRANSECT_KM."""
    az = np.radians(site["offshore_azimuth_deg"])
    d = np.arange(0.0, TRANSECT_KM + 1e-9, STEP_KM) - CHL_OFFSHORE_KM
    la = site["lat"] + np.cos(az) * d / KM_PER_DEG
    lo = site["lon"] + np.sin(az) * d / (KM_PER_DEG * np.cos(np.radians(site["lat"])))
    return la, lo, d + CHL_OFFSHORE_KM      # distance from shore


def sample(la, lo):
    """Nearest-pixel index for each transect point, and the values."""
    i = np.abs(lats[None, :] - la[:, None]).argmin(axis=1)
    j = np.abs(lons[None, :] - lo[:, None]).argmin(axis=1)
    inside = ((la >= lats.min()) & (la <= lats.max())
              & (lo >= lons.min()) & (lo <= lons.max()))
    vals = np.where(inside, field[i, j], np.nan)
    return i, j, vals, inside


sites = [(n, s) for n, s in CHL.items() if s.get("transect")]
ncol = 5
nrow = int(np.ceil(len(sites) / ncol))
fig, axes = plt.subplots(nrow, ncol, figsize=(4.0 * ncol, 4.3 * nrow),
                         facecolor="#fcfcfb")
fig.subplots_adjust(left=0.03, right=0.9, top=0.88, bottom=0.05,
                    wspace=0.22, hspace=0.22)
axes = np.atleast_1d(axes).ravel()

rows = []
for ax, (name, s) in zip(axes, sites):
    la, lo, dist = transect_points(s)
    i, j, vals, inside = sample(la, lo)

    pad = 0.13
    box = (min(lo.min(), s["lon"]) - pad, max(lo.max(), s["lon"]) + pad,
           min(la.min(), s["lat"]) - pad, max(la.max(), s["lat"]) + pad)
    mx = (lons >= box[0]) & (lons <= box[1])
    my = (lats >= box[2]) & (lats <= box[3])
    sub = np.ma.masked_invalid(field[np.ix_(my, mx)])
    x, y = lons[mx], lats[my]
    dx = (x[1] - x[0]) / 2 if len(x) > 1 else 0.001
    dy = (y[1] - y[0]) / 2 if len(y) > 1 else 0.001
    im = ax.imshow(sub, extent=[x.min() - dx, x.max() + dx,
                                y.min() + abs(dy), y.max() - abs(dy)],
                   origin="upper" if lats[0] > lats[-1] else "lower",
                   cmap="viridis", norm=LogNorm(vmin=VMIN, vmax=VMAX),
                   interpolation="nearest", aspect="auto")
    ax.set_facecolor("#111111")

    for seg in COAST:                       # OSM shoreline
        m = ((seg[:, 0] > box[0]) & (seg[:, 0] < box[1])
             & (seg[:, 1] > box[2]) & (seg[:, 1] < box[3]))
        if m.sum() > 1:
            ax.plot(seg[m, 0], seg[m, 1], color="#e34948", lw=1.0, alpha=0.9)

    ax.plot(lo, la, color="#ffffff", lw=0.9, alpha=0.85)
    uniq = sorted(set(zip(i.tolist(), j.tolist())))
    ax.scatter([lons[b] for _, b in uniq], [lats[a] for a, _ in uniq],
               s=9, marker="s", facecolors="none", edgecolors="#ffffff",
               linewidths=0.55, alpha=0.85)
    ax.plot(s["lon"], s["lat"], "o", ms=7, mfc="#eb6834", mec="#ffffff", mew=1.3,
            zorder=6)
    for d_mark in (5, 10, 15, 20):          # distance ticks along the line
        k = int(np.abs(dist - d_mark).argmin())
        ax.plot(lo[k], la[k], "|", ms=7, color="#ffffff", mew=1.2)

    n_valid = int(np.isfinite(vals).sum())
    ax.set_title(f"{name.replace('_', ' ')}", fontsize=10.5, color=INK,
                 loc="left", pad=5)
    ax.text(0.02, 0.97, f"{len(uniq)} pixels · {100 * n_valid / len(vals):.0f}% valid\n"
                        f"normal {s['offshore_azimuth_deg']:.0f}°",
            transform=ax.transAxes, fontsize=8, color="#e8e8e8", va="top")
    ax.tick_params(labelsize=7, colors=MUTED, length=2)
    for sp in ax.spines.values():
        sp.set_color(GRID)

    at_station = vals[int(np.abs(dist - CHL_OFFSHORE_KM).argmin())]
    rows.append((name, len(vals), len(uniq), n_valid,
                 at_station, np.nanmax(vals) if n_valid else np.nan,
                 np.nanmean(vals) if n_valid else np.nan))

for ax in axes[len(sites):]:
    ax.axis("off")

cax = fig.add_axes([0.915, 0.15, 0.011, 0.6])
cb = fig.colorbar(im, cax=cax)
cb.set_label("chlorophyll-a (mg m$^{-3}$), log scale", fontsize=9, color=MUTED)
cb.ax.tick_params(labelsize=8, colors=MUTED)

fig.suptitle(f"Transect sampling check - {CFG['label']}, {CFG['date']}",
             fontsize=14, color=INK, x=0.03, ha="left", y=0.975)
fig.text(0.03, 0.945, f"White line = shore-normal transect, 0-{TRANSECT_KM:.0f} km from the "
         f"OSM coastline (red). Squares = the pixels actually sampled. "
         f"Orange dot = station at {CHL_OFFSHORE_KM:.0f} km. Ticks every 5 km.",
         fontsize=9.5, color="#52514e", ha="left")
fig.savefig(OUT, dpi=105, facecolor=fig.get_facecolor())
print(f"wrote {OUT.name}")

print(f"\n{CFG['label']} · {CFG['date']}")
print(f"{'site':<16}{'steps':>7}{'pixels':>8}{'valid%':>8}{'at 4km':>9}"
      f"{'max':>8}{'mean':>8}")
for name, n, npix, nval, at, mx_, mn in rows:
    f = lambda v: "    -   " if not np.isfinite(v) else f"{v:8.2f}"
    print(f"{name:<16}{n:7d}{npix:8d}{100 * nval / n:8.0f}{f(at)}{f(mx_)}{f(mn)}")
