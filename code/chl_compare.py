"""
Compare the chlorophyll products available for the Nile -> Hadera corridor on a
single date, so the choice of product is made by looking rather than guessing.

Panels (all the same extent, same log colour scale):
    CMEMS multi-sensor  1 km    reprocessed, 1997-ongoing   <- candidate spine
    CMEMS OLCI          300 m   2016-ongoing
    NOAA VIIRS DINEOF   9 km    gap-filled, NASA-lineage algorithm
    NASA GIBS PACE OCI          the picture layer, NASA palette, for reference

Plus a coastal zoom that adds the 100 m Sentinel-2 mosaic and its turbidity
companion - the test of whether the red coastal band is phytoplankton or silt.

Inputs are the .nc files fetched alongside this script (see fetch commands in
the project notes); GIBS PNGs come straight from the WMS endpoint.

RUN:
    python chl_compare.py
"""

import pathlib

import numpy as np
import xarray as xr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import matplotlib.image as mpimg

from coast_points import POINTS

CACHE = pathlib.Path("/tmp")
OUT = pathlib.Path(__file__).with_name("chl_compare_2026-08-29.png")
OUT_ZOOM = pathlib.Path(__file__).with_name("chl_compare_coastal.png")

DATE = "2026-08-29"
BOX = (29.8, 35.4, 30.4, 33.3)          # lon0, lon1, lat0, lat1
ZOOM = (33.9, 34.9, 31.2, 32.1)
VMIN, VMAX = 0.03, 30.0                 # mg/m3, log scale

# Extra points worth sampling beyond the seven current stations.
EXTRA = {
    "Rosetta_mouth":  (31.47, 30.37),
    "Damietta_mouth": (31.52, 31.85),
    "Bardawil_off":   (31.30, 33.10),
    "Open_sea_ref":   (32.50, 33.50),
}

INK, MUTED, GRID = "#0b0b0b", "#898781", "#e1e0d9"


def load(fname, var):
    ds = xr.open_dataset(CACHE / fname)
    da = ds[var].squeeze()
    return da.values, ds.longitude.values, ds.latitude.values


def panel(ax, data, lons, lats, title, sub, cmap="viridis"):
    a = np.ma.masked_invalid(data)
    extent = [lons.min(), lons.max(), lats.min(), lats.max()]
    origin = "upper" if lats[0] > lats[-1] else "lower"
    im = ax.imshow(a, extent=extent, origin=origin, cmap=cmap,
                   norm=LogNorm(vmin=VMIN, vmax=VMAX), interpolation="nearest",
                   aspect="auto")
    ax.set_facecolor("#111111")
    valid = 100 * np.isfinite(data).mean()
    ax.set_title(title, fontsize=11, color=INK, loc="left", pad=6)
    ax.text(0.01, 0.965, f"{sub} · {valid:.0f}% valid pixels", transform=ax.transAxes,
            fontsize=8.5, color="#e8e8e8", va="top", ha="left")
    ax.tick_params(labelsize=7.5, colors=MUTED, length=2)
    for s in ax.spines.values():
        s.set_color(GRID)
    return im


def stations(ax, box):
    for name, (la, lo) in {**POINTS, **EXTRA}.items():
        if box[0] <= lo <= box[1] and box[2] <= la <= box[3]:
            ax.plot(lo, la, marker="D", ms=3.2, mfc="none", mec="#ffffff", mew=0.9)


# ----------------------------------------------------------------------
# Figure 1 - full corridor, four products
# ----------------------------------------------------------------------

fig, axes = plt.subplots(2, 2, figsize=(15, 9.2), facecolor="#fcfcfb")
fig.subplots_adjust(left=0.04, right=0.9, top=0.9, bottom=0.05, hspace=0.16, wspace=0.1)

c1, lo1, la1 = load("oc_1km.nc", "CHL")
im = panel(axes[0, 0], c1, lo1, la1, "CMEMS multi-sensor, 1 km",
           "reprocessed 1997-ongoing")
stations(axes[0, 0], BOX)

c3, lo3, la3 = load("oc_300m.nc", "CHL")
panel(axes[0, 1], c3, lo3, la3, "CMEMS Sentinel-3 OLCI, 300 m", "2016-ongoing")
stations(axes[0, 1], BOX)

ds = xr.open_dataset(CACHE / "viirs.nc", engine="scipy")
cv = np.squeeze(ds["chlor_a"].values)
panel(axes[1, 0], cv, ds.longitude.values, ds.latitude.values,
      "NOAA VIIRS DINEOF, 9 km", "gap-filled, NASA-lineage OCI algorithm")
stations(axes[1, 0], BOX)

gibs = mpimg.imread(CACHE / "gibs_OCI_PACE_Chlorophyll_a.png")
axes[1, 1].imshow(gibs, extent=[BOX[0], BOX[1], BOX[2], BOX[3]], aspect="auto")
axes[1, 1].set_facecolor("#111111")
axes[1, 1].set_title("NASA GIBS - PACE OCI (image layer)", fontsize=11, color=INK,
                     loc="left", pad=6)
axes[1, 1].text(0.01, 0.965, "NASA palette, not comparable values",
                transform=axes[1, 1].transAxes, fontsize=8.5, color="#e8e8e8", va="top")
axes[1, 1].tick_params(labelsize=7.5, colors=MUTED, length=2)
for s in axes[1, 1].spines.values():
    s.set_color(GRID)
stations(axes[1, 1], BOX)

cax = fig.add_axes([0.915, 0.12, 0.013, 0.62])
cb = fig.colorbar(im, cax=cax)
cb.set_label("chlorophyll-a (mg m$^{-3}$), log scale", fontsize=9, color=MUTED)
cb.ax.tick_params(labelsize=8, colors=MUTED)

fig.suptitle(f"Chlorophyll products compared - Nile to Hadera, {DATE}",
             fontsize=14, color=INK, x=0.04, ha="left", y=0.965)
fig.text(0.04, 0.928, "Diamonds mark the sampling points. Three left panels share "
         "one log scale; the NASA panel is an image, shown for reference only.",
         fontsize=9.5, color="#52514e", ha="left")
fig.savefig(OUT, dpi=110, facecolor=fig.get_facecolor())
print(f"wrote {OUT.name}")

# ----------------------------------------------------------------------
# Figure 2 - coastal zoom: is the red band plankton or silt?
# ----------------------------------------------------------------------

fig2, ax2 = plt.subplots(1, 4, figsize=(17, 5.4), facecolor="#fcfcfb")
fig2.subplots_adjust(left=0.03, right=0.91, top=0.84, bottom=0.08, wspace=0.14)


def crop(data, lons, lats, box):
    mx = (lons >= box[0]) & (lons <= box[1])
    my = (lats >= box[2]) & (lats <= box[3])
    return data[np.ix_(my, mx)], lons[mx], lats[my]


d, x, y = crop(c1, lo1, la1, ZOOM)
im2 = panel(ax2[0], d, x, y, "CMEMS 1 km", DATE)
stations(ax2[0], ZOOM)

d, x, y = crop(c3, lo3, la3, ZOOM)
panel(ax2[1], d, x, y, "CMEMS OLCI 300 m", DATE)
stations(ax2[1], ZOOM)

hr = xr.open_dataset(CACHE / "oc_100m_sep01.nc")
hchl = hr["CHL"].squeeze().values
htur = hr["TUR"].squeeze().values
panel(ax2[2], hchl, hr.longitude.values, hr.latitude.values,
      "Sentinel-2 mosaic 100 m - CHL", "2026-09-01 (29 Aug empty)")
stations(ax2[2], ZOOM)

a = np.ma.masked_invalid(htur)
imt = ax2[3].imshow(a, extent=[hr.longitude.values.min(), hr.longitude.values.max(),
                               hr.latitude.values.min(), hr.latitude.values.max()],
                    origin="upper" if hr.latitude.values[0] > hr.latitude.values[-1] else "lower",
                    cmap="magma", norm=LogNorm(vmin=0.3, vmax=30), aspect="auto")
ax2[3].set_facecolor("#111111")
ax2[3].set_title("Sentinel-2 mosaic 100 m - TURBIDITY", fontsize=11, color=INK,
                 loc="left", pad=6)
ax2[3].text(0.01, 0.965, "2026-09-01 · the silt cross-check", transform=ax2[3].transAxes,
            fontsize=8.5, color="#e8e8e8", va="top")
ax2[3].tick_params(labelsize=7.5, colors=MUTED, length=2)
for s in ax2[3].spines.values():
    s.set_color(GRID)
stations(ax2[3], ZOOM)

cax2 = fig2.add_axes([0.922, 0.12, 0.010, 0.6])
cb2 = fig2.colorbar(im2, cax=cax2)
cb2.set_label("chlorophyll-a (mg m$^{-3}$)", fontsize=9, color=MUTED)
cb2.ax.tick_params(labelsize=8, colors=MUTED)
cax3 = fig2.add_axes([0.962, 0.12, 0.010, 0.6])
cb3 = fig2.colorbar(imt, cax=cax3)
cb3.set_label("turbidity (FNU)", fontsize=9, color=MUTED)
cb3.ax.tick_params(labelsize=8, colors=MUTED)

fig2.suptitle("Coastal zoom - resolution, and whether the red band is plankton or silt",
              fontsize=14, color=INK, x=0.03, ha="left", y=0.965)
fig2.savefig(OUT_ZOOM, dpi=110, facecolor=fig2.get_facecolor())
print(f"wrote {OUT_ZOOM.name}")

# ----------------------------------------------------------------------
# Point comparison
# ----------------------------------------------------------------------


def sample(data, lons, lats, la, lo):
    if not (lons.min() <= lo <= lons.max() and lats.min() <= la <= lats.max()):
        return np.nan
    j = int(np.abs(lons - lo).argmin())
    i = int(np.abs(lats - la).argmin())
    return float(data[i, j])


print(f"\nchlorophyll (mg/m3) at each point, {DATE}")
print(f"{'point':<16}{'1 km':>9}{'300 m':>9}{'9 km':>9}")
for name, (la, lo) in {**POINTS, **EXTRA}.items():
    v1 = sample(c1, lo1, la1, la, lo)
    v3 = sample(c3, lo3, la3, la, lo)
    vv = sample(cv, ds.longitude.values, ds.latitude.values, la, lo)
    fmt = lambda v: "     -   " if not np.isfinite(v) else f"{v:9.2f}"
    print(f"{name:<16}{fmt(v1)}{fmt(v3)}{fmt(vv)}")
