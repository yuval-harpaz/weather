"""
Render one day's chlorophyll field twice: a clean image and an identical copy
with the stations and transects drawn on it, so the two can be flipped between.

RUN:
    python chl_day_image.py [YYYY-MM-DD] [sensor]

    sensor: multi1km (default) | olci300m | hr100m

OUTPUT:
    chl_<sensor>_<date>_<view>_plain.png
    chl_<sensor>_<date>_<view>_marked.png
"""

import json
import pathlib
import sys

import numpy as np
import xarray as xr
import copernicusmarine as cm
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from coast_points import CHL_OFFSHORE_KM

DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-08-25"
SENSOR = sys.argv[2] if len(sys.argv) > 2 else "multi1km"

DATASETS = {
    "multi1km": ("cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D", "multi-sensor 1 km"),
    "olci300m": ("cmems_obs-oc_med_bgc-plankton_my_l3-olci-300m_P1D", "OLCI 300 m"),
    "hr100m": ("cmems_obs_oc_med_bgc_tur-spm-chl_nrt_l3-hr-mosaic_P1D-m", "Sentinel-2 100 m"),
}
DATASET, LABEL = DATASETS[SENSOR]

VIEWS = {                       # name: (lon0, lon1, lat0, lat1)
    "corridor": (29.8, 35.4, 30.4, 33.3),
    "elarish": (33.4, 34.4, 30.9, 31.7),
}
TRANSECT_KM = 20.0
VMIN, VMAX = 0.03, 100.0
KM_PER_DEG = 111.19
INK, MUTED, GRID = "#0b0b0b", "#898781", "#e1e0d9"

HERE = pathlib.Path(__file__).parent
SITES = json.loads((HERE / "chl_points.json").read_text())
CACHE = pathlib.Path("/tmp") / f"chl_{SENSOR}_{DATE}.nc"

if not CACHE.exists():
    print(f"fetching {SENSOR} {DATE} ...")
    ds = cm.open_dataset(
        dataset_id=DATASET, variables=["CHL"],
        minimum_longitude=29.7, maximum_longitude=35.5,
        minimum_latitude=30.3, maximum_latitude=33.4,
        start_datetime=f"{DATE}T00:00:00", end_datetime=f"{DATE}T23:59:59")
    ds.to_netcdf(CACHE)
ds = xr.open_dataset(CACHE)
field = ds["CHL"].squeeze().values
lons, lats = ds.longitude.values, ds.latitude.values

osm = json.loads((pathlib.Path.home() / ".cache/weather/osm_coastline.json").read_text())
COAST = [np.array([[p["lon"], p["lat"]] for p in w["geometry"]])
         for w in osm["elements"] if w.get("geometry")]


def render(view, box, marked):
    mx = (lons >= box[0]) & (lons <= box[1])
    my = (lats >= box[2]) & (lats <= box[3])
    sub = np.ma.masked_invalid(field[np.ix_(my, mx)])
    x, y = lons[mx], lats[my]

    span = (box[1] - box[0]) / max(box[3] - box[2], 1e-9)
    h = 7.4
    fig, ax = plt.subplots(figsize=(h * span * 0.86 + 1.6, h), facecolor="#fcfcfb")
    fig.subplots_adjust(left=0.07, right=0.88, top=0.93, bottom=0.07)

    im = ax.imshow(sub, extent=[x.min(), x.max(), y.min(), y.max()],
                   origin="upper" if lats[0] > lats[-1] else "lower",
                   cmap="viridis", norm=LogNorm(vmin=VMIN, vmax=VMAX),
                   interpolation="nearest", aspect="auto")
    ax.set_facecolor("#111111")

    if marked:
        for seg in COAST:
            m = ((seg[:, 0] > box[0]) & (seg[:, 0] < box[1])
                 & (seg[:, 1] > box[2]) & (seg[:, 1] < box[3]))
            if m.sum() > 1:
                ax.plot(seg[m, 0], seg[m, 1], color="#e34948", lw=0.8, alpha=0.85)

        for name, s in SITES.items():
            if not (box[0] <= s["lon"] <= box[1] and box[2] <= s["lat"] <= box[3]):
                continue
            if s.get("transect"):
                az = np.radians(s["offshore_azimuth_deg"])

                def at(d):
                    return (s["lon"] + np.sin(az) * d
                            / (KM_PER_DEG * np.cos(np.radians(s["lat"]))),
                            s["lat"] + np.cos(az) * d / KM_PER_DEG)
                p0, p1 = at(-CHL_OFFSHORE_KM), at(TRANSECT_KM - CHL_OFFSHORE_KM)
                ax.plot([p0[0], p1[0]], [p0[1], p1[1]], color="#ffffff", lw=1.1,
                        alpha=0.9)
                ax.plot(*p0, "o", ms=3.5, mfc="#ffffff", mec="none")
            ax.plot(s["lon"], s["lat"], "o", ms=6.5, mfc="#eb6834", mec="#ffffff",
                    mew=1.3, zorder=6)
            ax.annotate(name.replace("_", " "), (s["lon"], s["lat"]),
                        textcoords="offset points", xytext=(8, 5), fontsize=8.5,
                        color="#ffffff")

    ax.tick_params(labelsize=8, colors=MUTED, length=2)
    for sp in ax.spines.values():
        sp.set_color(GRID)
    ax.set_title(f"Chlorophyll-a · {LABEL} · {DATE}"
                 + ("  — stations and transects marked" if marked else ""),
                 fontsize=12, color=INK, loc="left", pad=8)

    cax = fig.add_axes([0.895, 0.12, 0.016, 0.72])
    cb = fig.colorbar(im, cax=cax)
    cb.set_label("mg m$^{-3}$, log scale", fontsize=9, color=MUTED)
    cb.ax.tick_params(labelsize=8, colors=MUTED)

    out = HERE / f"chl_{SENSOR}_{DATE}_{view}_{'marked' if marked else 'plain'}.png"
    fig.savefig(out, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out


for view, box in VIEWS.items():
    for marked in (False, True):
        print("wrote", render(view, box, marked).name)

print(f"\nvalues at the sites on {DATE} ({LABEL})")
print(f"{'site':<16}{'CHL':>9}")
for name, s in SITES.items():
    j = int(np.abs(lons - s["lon"]).argmin())
    i = int(np.abs(lats - s["lat"]).argmin())
    v = field[i, j]
    print(f"{name:<16}{'      -  ' if not np.isfinite(v) else f'{v:9.2f}'}")
