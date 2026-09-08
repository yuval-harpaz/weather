"""
Animate the chlorophyll field over a date range, with the sampling transects and
their daily p80 drawn on top - so the numbers in the CSV can be checked against
the imagery they came from.

One frame per day, one second per frame. Days with no usable image keep the
previous day's picture, labelled so the repeat is obvious rather than looking
like a frozen video.

RUN:
    python chl_movie.py 2025-08-01 2025-09-30
    python chl_movie.py 2026-08-01 2026-09-08 -o summer2026.mp4

    Reprocessed data is used wherever it exists and near-real-time fills the
    tail, the same rule the CSV update follows.

OUTPUT:
    chl_movie_<start>_<end>.mp4
"""

import argparse
import datetime as dt
import pathlib
import shutil
import subprocess
import tempfile

import numpy as np
import xarray as xr
import copernicusmarine as cm
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.colors import LogNorm

from chl_transects import (SITES, SENSORS, record_end, transect_latlon,
                           CHL_OFFSHORE_KM, TRANSECT_KM)

MARGIN_DEG = 0.25
MIN_VALID = 0.50        # a line with more than half its sea pixels missing is not reported
VMIN, VMAX = 0.05, 50.0
FPS = 1
KM_PER_DEG = 111.19
INK, MUTED = "#0b0b0b", "#898781"
HALO = [pe.withStroke(linewidth=2.2, foreground="#000000")]


def parse_args():
    a = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    a.add_argument("start", help="first day, yyyy-mm-dd")
    a.add_argument("end", help="last day, yyyy-mm-dd")
    a.add_argument("-o", "--out", default=None, help="output mp4")
    a.add_argument("--keep-frames", action="store_true",
                   help="leave the PNG frames on disk")
    return a.parse_args()


def box_for_sites():
    lats, lons = [], []
    for name, s in SITES.items():
        if s.get("transect"):
            la, lo, _ = transect_latlon(s)
            lats += [la.min(), la.max()]
            lons += [lo.min(), lo.max()]
        else:
            lats.append(s["lat"])
            lons.append(s["lon"])
    return (min(lons) - MARGIN_DEG, max(lons) + MARGIN_DEG,
            min(lats) - MARGIN_DEG, max(lats) + MARGIN_DEG)


def load_field(start, end, box):
    """CHL over the range: reprocessed where it exists, near-real-time after."""
    my_end = record_end("multi1km")
    parts = []

    my_last = min(end, my_end)
    if start <= my_last:
        print(f"  reprocessed {start} -> {my_last}")
        parts.append(cm.open_dataset(
            dataset_id=SENSORS["multi1km"]["dataset"], variables=["CHL"],
            minimum_longitude=box[0], maximum_longitude=box[1],
            minimum_latitude=box[2], maximum_latitude=box[3],
            start_datetime=f"{start}T00:00:00",
            end_datetime=f"{my_last}T00:00:00").load()["CHL"])

    if end > my_end:
        nrt_first = (dt.date.fromisoformat(my_end) + dt.timedelta(days=1)).isoformat()
        nrt_start = max(start, nrt_first)
        nrt_end = min(end, record_end("nrt1km"))
        if nrt_start <= nrt_end:
            print(f"  near real time {nrt_start} -> {nrt_end}")
            parts.append(cm.open_dataset(
                dataset_id=SENSORS["nrt1km"]["dataset"], variables=["CHL"],
                minimum_longitude=box[0], maximum_longitude=box[1],
                minimum_latitude=box[2], maximum_latitude=box[3],
                start_datetime=f"{nrt_start}T00:00:00",
                end_datetime=f"{nrt_end}T00:00:00").load()["CHL"])

    if not parts:
        raise SystemExit("nothing available for that range")
    field = xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]
    return field.sortby("time")


def transect_pixels(site, lats, lons):
    """Row/column indices along the transect, plus distance from shore."""
    la, lo, dist = transect_latlon(site)
    i = np.abs(lats[None, :] - la[:, None]).argmin(axis=1)
    j = np.abs(lons[None, :] - lo[:, None]).argmin(axis=1)
    return i, j, dist, la, lo


def main():
    args = parse_args()
    start, end = args.start, args.end
    out = pathlib.Path(args.out or f"chl_movie_{start}_{end}.mp4")

    box = box_for_sites()
    print(f"box {box[0]:.2f}..{box[1]:.2f} E, {box[2]:.2f}..{box[3]:.2f} N")
    field = load_field(start, end, box)
    lats, lons = field.latitude.values, field.longitude.values
    cube = field.values
    have = {np.datetime_as_string(t, unit="D"): k
            for k, t in enumerate(field.time.values)}
    print(f"  {len(have)} days with imagery of "
          f"{(dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days + 1} requested")

    # geometry and the sea mask, both fixed for the whole run
    geom = {}
    for name, s in SITES.items():
        if not s.get("transect"):
            continue
        i, j, dist, la, lo = transect_pixels(s, lats, lons)
        sea = np.array([np.isfinite(cube[:, a, b]).any() for a, b in zip(i, j)])
        k_st = int(np.abs(dist - CHL_OFFSHORE_KM).argmin())     # the station itself
        geom[name] = dict(i=i, j=j, dist=dist, la=la, lo=lo, sea=sea, k_st=k_st)

    frames = pathlib.Path(tempfile.mkdtemp(prefix="chlframes_"))
    span = (box[1] - box[0]) / max(box[3] - box[2], 1e-9)
    day = dt.date.fromisoformat(start)
    last = dt.date.fromisoformat(end)
    n, carried, last_good = 0, 0, None

    while day <= last:
        iso = day.isoformat()
        idx = have.get(iso)
        frame_of = iso
        if idx is None or not np.isfinite(cube[idx]).any():
            idx, frame_of = (None, None) if last_good is None else last_good
            carried += 1
        else:
            last_good = (idx, iso)

        # aspect from the latitude, so the coast is not stretched, and enough
        # margin that the degree labels are not clipped
        yx = 1.0 / np.cos(np.radians(0.5 * (box[2] + box[3])))
        fig, ax = plt.subplots(
            figsize=(12.4, 12.4 * 0.86 / span * yx + 0.9), facecolor="#111111")
        fig.subplots_adjust(left=0.045, right=0.90, top=0.975, bottom=0.075)
        ax.set_facecolor("#111111")

        if idx is not None:
            ax.imshow(np.ma.masked_invalid(cube[idx]),
                      extent=[lons.min(), lons.max(), lats.min(), lats.max()],
                      origin="upper" if lats[0] > lats[-1] else "lower",
                      cmap="viridis", norm=LogNorm(vmin=VMIN, vmax=VMAX),
                      interpolation="nearest", aspect=yx)

        for name, g in geom.items():
            ax.plot(g["lo"], g["la"], color="#ffffff", lw=1.0, alpha=0.85)
            # the station is 4 km offshore, not at the shore end of the line
            k = g["k_st"]
            ax.plot(g["lo"][k], g["la"][k], "o", ms=5, mfc="#eb6834",
                    mec="#ffffff", mew=1.0, zorder=5)
            ax.text(g["lo"][k], g["la"][k], "  " + name.replace("_", " "),
                    color="#dddddd", fontsize=7.5, va="center", ha="left",
                    path_effects=HALO)
            # p80 over the sea pixels of this transect, written at the deep end.
            # A line that has lost more than half its pixels is not reported at
            # all - a p80 over two surviving pixels is not the same statistic.
            val = np.nan
            if idx is not None:
                vals = cube[idx][g["i"], g["j"]][g["sea"]]
                if g["sea"].sum() and np.isfinite(vals).mean() >= MIN_VALID:
                    val = np.nanpercentile(vals, 80)
            ax.text(g["lo"][-1], g["la"][-1], "  -  " if not np.isfinite(val)
                    else f"  {val:.1f}", color="#ffffff", fontsize=9.5,
                    fontweight="bold", va="center", ha="left", path_effects=HALO)

        for name, s in SITES.items():          # sites without a transect
            if s.get("transect"):
                continue
            ax.plot(s["lon"], s["lat"], "o", ms=5, mfc="none", mec="#ffffff", mew=1.2)
            v = cube[idx][np.abs(lats - s["lat"]).argmin(),
                          np.abs(lons - s["lon"]).argmin()] if idx is not None else np.nan
            ax.text(s["lon"], s["lat"], f"  {name.replace('_', ' ')} "
                    + ("-" if not np.isfinite(v) else f"{v:.2f}"),
                    color="#dddddd", fontsize=7.5, va="center", path_effects=HALO)

        ax.text(0.012, 0.982, day.strftime("%d %b %Y"), transform=ax.transAxes,
                color="#ffffff", fontsize=19, fontweight="bold", va="top",
                path_effects=HALO)
        if frame_of != iso:
            note = "no image" if frame_of is None else f"no image · showing {frame_of}"
            ax.text(0.012, 0.936, note, transform=ax.transAxes, color="#ffbdbd",
                    fontsize=11, va="top", path_effects=HALO)

        ax.set_aspect(yx)
        ax.set_xlim(box[0], box[1])
        ax.set_ylim(box[2], box[3])
        ax.tick_params(labelsize=7, colors=MUTED, length=2)
        for sp in ax.spines.values():
            sp.set_color("#333333")

        sm = plt.cm.ScalarMappable(cmap="viridis", norm=LogNorm(vmin=VMIN, vmax=VMAX))
        cb = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.015)
        cb.set_label("chlorophyll-a (mg m$^{-3}$)", fontsize=9, color="#cccccc")
        cb.ax.tick_params(labelsize=8, colors="#cccccc")

        fig.savefig(frames / f"f{n:05d}.png", dpi=96, facecolor=fig.get_facecolor())
        plt.close(fig)
        n += 1
        day += dt.timedelta(days=1)

    print(f"  {n} frames, {carried} carried over from an earlier day")

    cmd = ["ffmpeg", "-y", "-loglevel", "warning", "-framerate", str(FPS),
           "-i", str(frames / "f%05d.png"),
           "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
           "-c:v", "libx264", "-profile:v", "high", "-pix_fmt", "yuv420p",
           "-crf", "20", "-r", "30", "-movflags", "+faststart", str(out)]
    subprocess.run(cmd, check=True)
    if not args.keep_frames:
        shutil.rmtree(frames, ignore_errors=True)
    else:
        print(f"  frames kept in {frames}")
    print(f"wrote {out} ({out.stat().st_size / 1e6:.1f} MB, {n} s)")


if __name__ == "__main__":
    main()
