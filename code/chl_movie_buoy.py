"""
One-off: the chlorophyll field around Hadera and the Sdot Yam buoy, day by day,
with the buoy's own in-water reading running along the bottom.

Written to answer one question - does the satellite pixel over the buoy behave
like the buoy's fluorometer? - so it hardcodes the two points and the window
rather than taking arguments. chl_movie.py is the general tool.

The exact pixel over the buoy carries data on only about a third of days, being
close enough to shore to be masked often, so the 3x3 median around it is drawn
as well; that reaches about two thirds of days.

RUN:
    python chl_movie_buoy.py

    Needs the Copernicus Marine credentials the other chl scripts use, and
    ffmpeg on the path.

OUTPUT:
    ~/Documents/chl_buoy_hadera.mp4
"""

import datetime as dt
import os
import pathlib
import subprocess
import tempfile

import numpy as np
import pandas as pd
import xarray as xr
import copernicusmarine as cm
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.colors import LogNorm

from chl_transects import SENSORS, record_end, CREDS

BUOY = (32.49463, 34.87989)             # the buoy's own GPS
HADERA = (32.4765, 34.8389)             # the transect station, 4 km offshore
START, END = "2026-06-15", "2026-09-13"
BOX = (34.72, 35.00, 32.38, 32.60)      # lon0, lon1, lat0, lat1
VMIN, VMAX = 0.05, 3.0                  # this patch never approaches the open
FPS = 2                                 # basin's range, so the scale is tighter
OUT = pathlib.Path(os.path.expanduser("~/Documents/chl_buoy_hadera.mp4"))
INK, MUTED = "#0b0b0b", "#898781"
# One hue per instrument: green is the thing in the water, blue is everything
# the satellite saw. The three satellite series are then told apart by dash
# rather than by colour - green against blue separates well for red-green colour
# blindness (dE 18) but poorly for the much rarer blue-yellow kind (dE 3.5), and
# three more hues would have made that worse rather than better.
BUOY_C, SAT_C = "#2eaa60", "#3f9ad8"
HALO = [pe.withStroke(linewidth=2.4, foreground="#000000")]


def load_field():
    """Reprocessed where it exists, near-real-time after - the CSV's own rule."""
    my_end = record_end("multi1km")
    parts, my_last = [], min(END, my_end)
    if START <= my_last:
        parts.append(cm.open_dataset(
            dataset_id=SENSORS["multi1km"]["dataset"], variables=["CHL"],
            minimum_longitude=BOX[0], maximum_longitude=BOX[1],
            minimum_latitude=BOX[2], maximum_latitude=BOX[3],
            start_datetime=f"{START}T00:00:00",
            end_datetime=f"{my_last}T00:00:00", **CREDS).load()["CHL"])
    if END > my_end:
        first = (dt.date.fromisoformat(my_end) + dt.timedelta(days=1)).isoformat()
        s2, e2 = max(START, first), min(END, record_end("nrt1km"))
        if s2 <= e2:
            parts.append(cm.open_dataset(
                dataset_id=SENSORS["nrt1km"]["dataset"], variables=["CHL"],
                minimum_longitude=BOX[0], maximum_longitude=BOX[1],
                minimum_latitude=BOX[2], maximum_latitude=BOX[3],
                start_datetime=f"{s2}T00:00:00",
                end_datetime=f"{e2}T00:00:00", **CREDS).load()["CHL"])
    field = xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]
    return field.sortby("time")


def buoy_series():
    """The fluorometer's daily median, from the saved 90-day pull."""
    import json
    raw = json.load(open("/tmp/buoy_chlorophyll_ugL.json"))["data"]
    f = pd.DataFrame(raw)
    f["t"] = pd.to_datetime(f["timestamp"]) + pd.Timedelta(hours=3)
    s = (f.assign(v=pd.to_numeric(f["value"], errors="coerce"))
          .dropna(subset=["v"]).set_index("t")["v"].resample("D").median())
    s.index = s.index.date
    return s


def hadera_series():
    """The transect station's own numbers, straight from the committed CSV."""
    d = pd.read_csv(pathlib.Path(__file__).parent.parent
                    / "data" / "chl_transect_multi1km_2026.csv")
    d = d[(d["site"] == "Hadera_IL") & (d["variable"] == "CHL")]
    d = d.set_index("date")
    return d["at_station"], d["transect_p80"]


def main():
    field = load_field()
    lats, lons = field.latitude.values, field.longitude.values
    cube = field.values
    days = list(pd.to_datetime(field.time.values).date)
    i = int(np.abs(lats - BUOY[0]).argmin())
    j = int(np.abs(lons - BUOY[1]).argmin())
    sat3 = np.nanmedian(cube[:, i - 1:i + 2, j - 1:j + 2].reshape(len(cube), -1),
                        axis=1)
    buoy = buoy_series()
    buoy_v = np.array([buoy.get(d, np.nan) for d in days])
    at_st, p80 = hadera_series()
    had_v = np.array([at_st.get(str(d), np.nan) for d in days], dtype=float)
    p80_v = np.array([p80.get(str(d), np.nan) for d in days], dtype=float)

    extent = [lons[0], lons[-1], lats[0], lats[-1]]
    tmp = pathlib.Path(tempfile.mkdtemp())
    last_good, kept = None, 0

    for n, day in enumerate(days):
        frame = cube[n]
        stale = ""
        if np.isfinite(frame).sum() < 8:            # almost nothing visible
            if last_good is None:
                continue
            frame, stale, kept = last_good[1], f"  (image from {last_good[0]})", kept + 1
        else:
            last_good = (day, frame)

        fig = plt.figure(figsize=(7.6, 9.4))
        fig.patch.set_facecolor("#0d0d0d")
        gs = fig.add_gridspec(2, 1, height_ratios=[2.2, 1.1], hspace=.18,
                              left=.10, right=.97, top=.94, bottom=.08)
        ax = fig.add_subplot(gs[0])
        ax.imshow(frame, origin="lower", extent=extent, norm=LogNorm(VMIN, VMAX),
                  cmap="viridis", interpolation="nearest")
        ax.set_facecolor("#161616")
        for (la, lo), name, mark in ((HADERA, "Hadera station", "o"),
                                     (BUOY, "Sdot Yam buoy", "s")):
            ax.plot(lo, la, mark, mfc="none", mec="#ffffff", mew=2, ms=13)
            ax.annotate(name, (lo, la), color="#ffffff", fontsize=10.5,
                        textcoords="offset points", xytext=(15, -4),
                        path_effects=HALO)
        ax.set_title(f"Chlorophyll   {day}{stale}", color="#ffffff",
                     fontsize=13, loc="left")
        ax.tick_params(colors=MUTED, labelsize=8)
        for s in ax.spines.values():
            s.set_color("#2c2c2a")

        bx = fig.add_subplot(gs[1])
        bx.set_facecolor("#0d0d0d")
        bx.plot(days, buoy_v, color=BUOY_C, lw=1.8,
                label="buoy fluorometer, µg/L")
        bx.plot(days, had_v, color=SAT_C, lw=1.5,
                label="Hadera at_station, mg/m³")
        bx.plot(days, p80_v, color=SAT_C, lw=1.3, ls="--",
                label="Hadera transect p80, mg/m³")
        bx.plot(days, sat3, color=SAT_C, lw=1.3, ls=":",
                label="over the buoy, 3×3 median, mg/m³")
        bx.set_yscale("log")
        # One axis, not two: a log scale holds both honestly even though the
        # fluorometer reads about eighty times higher than the satellite.
        bx.axvline(day, color="#e05252", lw=1.6)
        v = buoy.get(day, np.nan)
        if np.isfinite(v):
            bx.plot([day], [v], "o", color="#e05252", ms=7, zorder=5)
        bx.set_xlim(days[0], days[-1])
        bx.tick_params(colors=MUTED, labelsize=8)
        bx.grid(color="#2c2c2a", lw=.6)
        bx.legend(frameon=False, fontsize=7.8, labelcolor="#c3c2b7",
                  loc="upper left", ncol=2, columnspacing=1.2,
                  handlelength=2.6)
        for s in bx.spines.values():
            s.set_color("#2c2c2a")

        fig.savefig(tmp / f"f{n:04d}.png", dpi=110, facecolor=fig.get_facecolor())
        plt.close(fig)

    frames = sorted(tmp.glob("f*.png"))
    print(f"{len(frames)} frames ({kept} repeated for want of a usable image)")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "warning", "-framerate", str(FPS),
                    "-pattern_type", "glob", "-i", str(tmp / "f*.png"),
                    "-c:v", "libx264", "-pix_fmt", "yuv420p",
                    "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2", str(OUT)], check=True)
    print(f"wrote {OUT} ({OUT.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
