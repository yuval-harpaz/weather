"""
Show the standardised 4 km chlorophyll sites on a real scene, to check they sit
in the green band rather than in the surf or out in the blue.

Uses the OLCI 300 m field already fetched for 2026-08-29.

RUN:
    python chl_points_map.py
"""

import json
import pathlib

import numpy as np
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from coast_points import POINTS, CHL_OFFSHORE_KM

CHL = json.loads(pathlib.Path(__file__).with_name("chl_points.json").read_text())
OUT = pathlib.Path(__file__).with_name("chl_points_2026-08-29.png")

FULL = (29.9, 35.4, 30.5, 33.3)
ZOOM = (33.9, 35.0, 31.1, 32.2)
TRANSECT_KM = 20.0
VMIN, VMAX = 0.03, 30.0
KM_PER_DEG = 111.19
INK, MUTED, GRID = "#0b0b0b", "#898781", "#e1e0d9"

ds = xr.open_dataset("/tmp/oc_300m.nc")
chl = ds["CHL"].squeeze().values
lons, lats = ds.longitude.values, ds.latitude.values


def draw(ax, box, label_sites, transects):
    mx = (lons >= box[0]) & (lons <= box[1])
    my = (lats >= box[2]) & (lats <= box[3])
    d = np.ma.masked_invalid(chl[np.ix_(my, mx)])
    x, y = lons[mx], lats[my]
    im = ax.imshow(d, extent=[x.min(), x.max(), y.min(), y.max()],
                   origin="upper" if lats[0] > lats[-1] else "lower",
                   cmap="viridis", norm=LogNorm(vmin=VMIN, vmax=VMAX),
                   interpolation="nearest", aspect="auto")
    ax.set_facecolor("#111111")

    for name, s in CHL.items():
        if not (box[0] <= s["lon"] <= box[1] and box[2] <= s["lat"] <= box[3]):
            continue
        if transects and s.get("transect"):
            az = np.radians(s["offshore_azimuth_deg"])
            # transect runs from the shore anchor outward through the site
            def at(d_km):
                return (s["lon"] + np.sin(az) * d_km / (KM_PER_DEG * np.cos(np.radians(s["lat"]))),
                        s["lat"] + np.cos(az) * d_km / KM_PER_DEG)
            p0, p1 = at(-CHL_OFFSHORE_KM), at(TRANSECT_KM - CHL_OFFSHORE_KM)
            ax.plot([p0[0], p1[0]], [p0[1], p1[1]], color="#ffffff", lw=1.0, alpha=0.8)
            ax.plot(*p0, "o", ms=3.5, mfc="#ffffff", mec="none", zorder=5)  # shore anchor
        ax.plot(s["lon"], s["lat"], "o", ms=6, mfc="#eb6834", mec="#ffffff", mew=1.2,
                zorder=5)
        if label_sites:
            ax.annotate(name.replace("_", " "), (s["lon"], s["lat"]),
                        textcoords="offset points", xytext=(8, 4), fontsize=8.5,
                        color="#ffffff")

    for name, (la, lo) in POINTS.items():       # where they were, for contrast
        if box[0] <= lo <= box[1] and box[2] <= la <= box[3]:
            ax.plot(lo, la, "D", ms=5, mfc="none", mec="#9ecbff", mew=1.1, zorder=4)

    ax.tick_params(labelsize=8, colors=MUTED, length=2)
    for sp in ax.spines.values():
        sp.set_color(GRID)
    return im


fig, axes = plt.subplots(1, 2, figsize=(17, 6.4), facecolor="#fcfcfb",
                         gridspec_kw={"width_ratios": [1.55, 1]})
fig.subplots_adjust(left=0.035, right=0.9, top=0.86, bottom=0.07, wspace=0.12)

im = draw(axes[0], FULL, label_sites=True, transects=False)
axes[0].set_title("Nile to Hadera - all sites at 4 km offshore", fontsize=11,
                  color=INK, loc="left", pad=6)
draw(axes[1], ZOOM, label_sites=False, transects=True)
axes[1].set_title(f"Coastal zoom - white lines are the 0-{TRANSECT_KM:.0f} km transects",
                  fontsize=11, color=INK, loc="left", pad=6)

cax = fig.add_axes([0.915, 0.13, 0.012, 0.66])
cb = fig.colorbar(im, cax=cax)
cb.set_label("chlorophyll-a (mg m$^{-3}$), log scale", fontsize=9, color=MUTED)
cb.ax.tick_params(labelsize=8, colors=MUTED)

fig.suptitle("Standardised chlorophyll sites on the 2026-08-29 OLCI 300 m scene",
             fontsize=14, color=INK, x=0.035, ha="left", y=0.965)
fig.text(0.035, 0.925, "Orange dots = new 4 km sites.  Blue diamonds = the current-analysis "
         "stations, which stay on their isobath and are shown only for contrast.",
         fontsize=9.5, color="#52514e", ha="left")
fig.savefig(OUT, dpi=110, facecolor=fig.get_facecolor())
print(f"wrote {OUT.name}")

# what each site actually reads on this scene
print(f"\n{'site':<16}{'CHL':>8}   depth  dist_km")
for name, s in CHL.items():
    j = int(np.abs(lons - s["lon"]).argmin())
    i = int(np.abs(lats - s["lat"]).argmin())
    v = chl[i, j]
    if not (lons.min() <= s["lon"] <= lons.max()):
        v = np.nan
    print(f"{name:<16}{'   -  ' if not np.isfinite(v) else f'{v:8.2f}'}"
          f"{s.get('model_depth_m', float('nan')):8.1f}{s['distance_to_shore_km']:9.2f}")
