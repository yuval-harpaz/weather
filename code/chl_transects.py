"""
Sample chlorophyll along each site's shore-normal transect, one sensor at a time.

For every site and every day it records the value at the station (4 km offshore),
plus the max and mean along a transect running from the shore out to 20 km - so a
plume that shifts position is still measured, which a fixed point cannot do.

Gap accounting is done only over sea pixels. These products return NaN for land
and for cloud alike, so a transect pixel counts as sea if it is valid at least
once during the month; land then never counts against a sensor's score.

RUN:
    python chl_transects.py [sensor ...]        # default: all, in order

OUTPUT (per sensor and month, in ../data):
    chl_transect_<sensor>_<YYYY>.csv            daily summary per site
    chl_profile_<sensor>_<YYYY>.parquet         full distance x day profiles
"""

import json
import pathlib
import sys
import time
import calendar

import datetime as dt
import os

import numpy as np
import pandas as pd
import xarray as xr
import copernicusmarine as cm

from coast_points import CHL_OFFSHORE_KM

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
SITES = json.loads((HERE / "chl_points.json").read_text())

SENSORS = {
    "olci300m": dict(
        dataset="cmems_obs-oc_med_bgc-plankton_my_l3-olci-300m_P1D",
        variables=["CHL"], res_km=0.30, label="Sentinel-3 OLCI 300 m",
        stream="my"),
    "multi1km": dict(
        dataset="cmems_obs-oc_med_bgc-plankton_my_l3-multi-1km_P1D",
        variables=["CHL"], res_km=1.00, label="multi-sensor 1 km", stream="my"),
    # Same product, near-real-time stream: about a week fresher than the
    # reprocessed one but only a ~13 day rolling window, so it can fill the tail
    # and nothing more. Measured against MY over their overlap the two agree to
    # better than 0.5%, which is why the rows can share one file.
    "nrt1km": dict(
        dataset="cmems_obs-oc_med_bgc-plankton_nrt_l3-multi-1km_P1D",
        variables=["CHL"], res_km=1.00, label="multi-sensor 1 km (NRT)",
        stream="nrt", fills="multi1km"),
    "hr100m": dict(
        dataset="cmems_obs_oc_med_bgc_tur-spm-chl_nrt_l3-hr-mosaic_P1D-m",
        variables=["CHL", "TUR"], res_km=0.10, label="Sentinel-2 mosaic 100 m",
        stream="nrt"),
}
# Yearly chunks: the per-request overhead dominates, so one request per site
# per year is far cheaper than twelve monthly ones for the same pixels.
YEARS = list(range(2015, 2027))

TRANSECT_KM = 20.0
STEP_KM = 0.1
KM_PER_DEG = 111.19
MARGIN_DEG = 0.05
# A transect that has lost more than half its sea pixels is not reported: a p80
# over two surviving pixels is a different statistic from one over eighty.
MIN_VALID = 0.50
# Fallback only. The real end of each record is read from the catalogue at run
# time - hardcoding it would quietly freeze an automated updater the moment the
# reprocessed stream moved on.
MY_LAST_DAY = "2026-08-29"
_END_CACHE = {}


def record_end(sensor):
    """Last day actually available for this dataset, from the CMEMS catalogue."""
    if sensor in _END_CACHE:
        return _END_CACHE[sensor]
    end = MY_LAST_DAY
    try:
        cat = cm.describe(dataset_id=SENSORS[sensor]["dataset"],
                          disable_progress_bar=True)
        ds = cat.products[0].datasets[0]
        svc = [sv for v in ds.versions for pt in v.parts for sv in pt.services
               if "arco-time" in str(sv.service_name)][0]
        co = [c for c in svc.variables[0].coordinates
              if c.coordinate_id == "time"][0]
        end = dt.datetime.fromtimestamp(co.maximum_value / 1000,
                                        dt.UTC).strftime("%Y-%m-%d")
    except Exception as exc:
        print(f"  (could not read the record end: {type(exc).__name__}; "
              f"falling back to {end})")
    _END_CACHE[sensor] = end
    return end


def credentials():
    """Copernicus Marine credentials, from the environment or the stored file.

    Passed explicitly to every request rather than left to the library's own
    lookup: when neither source is present it prompts on stdin, which means a
    script appears to hang and a CI job blocks until it times out.
    """
    user = os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME")
    password = os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")
    if user and password:
        return {"username": user, "password": password}
    if (pathlib.Path.home() / ".copernicusmarine"
            / ".copernicusmarine-credentials").exists():
        return {}                       # the library will read the file itself
    raise SystemExit(
        "No Copernicus Marine credentials found.\n"
        "  export COPERNICUSMARINE_SERVICE_USERNAME=...\n"
        "  export COPERNICUSMARINE_SERVICE_PASSWORD=...\n"
        "They are in ~/.profile, so a login shell (bash -l) picks them up; a "
        "plain IDE terminal may not. Alternatively run `copernicusmarine login` "
        "once to store them.")


CREDS = credentials()


def transect_latlon(site):
    """Points every STEP_KM from the shore anchor out to TRANSECT_KM."""
    az = np.radians(site["offshore_azimuth_deg"])
    d = np.arange(0.0, TRANSECT_KM + 1e-9, STEP_KM) - CHL_OFFSHORE_KM
    la = site["lat"] + np.cos(az) * d / KM_PER_DEG
    lo = site["lon"] + np.sin(az) * d / (KM_PER_DEG * np.cos(np.radians(site["lat"])))
    return la, lo, d + CHL_OFFSHORE_KM


def period_range(period, last_day):
    """(start, end) for a year 'YYYY' or a month 'YYYY-MM', clipped to the record."""
    if "-" in str(period):
        y, m = (int(v) for v in str(period).split("-"))
        first = f"{y}-{m:02d}-01"
        last = f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"
    else:
        y = int(period)
        first, last = f"{y}-01-01", f"{y}-12-31"
    return first, min(last, last_day)


def fetch(cfg, box, start, end):
    # These daily fields are stamped at 00:00, so asking for T23:59:59 overshoots
    # the last available stamp by a day and earns a warning per site per run.
    # T00:00:00 still includes that day.
    ds = cm.open_dataset(
        dataset_id=cfg["dataset"], variables=cfg["variables"],
        minimum_longitude=box[0], maximum_longitude=box[1],
        minimum_latitude=box[2], maximum_latitude=box[3],
        start_datetime=f"{start}T00:00:00", end_datetime=f"{end}T00:00:00",
        **CREDS,
    )
    return ds.load()


def collect(sensor, start, end, quiet=False, sites=None):
    """Sample sites between two dates. Returns (summary, profiles, timings)."""
    cfg = SENSORS[sensor]
    t_down = t_agg = 0.0
    summary, profiles = [], []

    for name, site in SITES.items():
        if sites is not None and name not in sites:
            continue
        has_transect = bool(site.get("transect"))
        if has_transect:
            la, lo, dist = transect_latlon(site)
        else:                                   # open-sea reference: point only
            la = np.array([site["lat"]])
            lo = np.array([site["lon"]])
            dist = np.array([0.0])

        box = (lo.min() - MARGIN_DEG, lo.max() + MARGIN_DEG,
               la.min() - MARGIN_DEG, la.max() + MARGIN_DEG)

        t0 = time.perf_counter()
        try:
            ds = fetch(cfg, box, start, end)
        except Exception as exc:
            print(f"  {name:<16} FETCH FAILED: {type(exc).__name__}: {exc}")
            continue
        t_down += time.perf_counter() - t0

        t0 = time.perf_counter()
        lats, lons = ds.latitude.values, ds.longitude.values
        i = np.abs(lats[None, :] - la[:, None]).argmin(axis=1)
        j = np.abs(lons[None, :] - lo[:, None]).argmin(axis=1)
        times = pd.to_datetime(ds.time.values)

        for var in cfg["variables"]:
            cube = ds[var].values                       # (time, lat, lon)
            series = cube[:, i, j]                      # (time, n_steps)
            # A transect pixel is sea if it is ever valid this month.
            uniq = {}
            for k, (a, b) in enumerate(zip(i, j)):
                uniq.setdefault((a, b), []).append(k)
            is_sea = np.zeros(len(la), dtype=bool)
            for (a, b), ks in uniq.items():
                if np.isfinite(cube[:, a, b]).any():
                    is_sea[ks] = True
            n_sea = int(is_sea.sum())

            k_station = int(np.abs(dist - CHL_OFFSHORE_KM).argmin())
            for t, row in zip(times, series):
                sea = row[is_sea] if n_sea else row
                valid = np.isfinite(sea)
                enough = valid.any() and (not has_transect
                                          or valid.mean() >= MIN_VALID)
                summary.append(dict(
                    date=t.date(), site=name, variable=var, sensor=sensor,
                    stream=cfg.get("stream", "my"),
                    at_station=row[k_station] if has_transect else row[0],
                    # p80, not the maximum: a single land-adjacent pixel can read
                    # 100+ mg/m3 next to a neighbour at 12, and the max reports
                    # that pixel. p80 lands offshore of the contaminated strip.
                    transect_p80=(np.nanpercentile(sea, 80) if enough
                                  else np.nan),
                    transect_mean=np.nanmean(sea) if enough else np.nan,
                    transect_max=np.nanmax(sea) if enough else np.nan,
                    n_valid=int(valid.sum()), n_sea=n_sea,
                    valid_frac=round(float(valid.mean()), 4) if n_sea else np.nan,
                    n_pixels=len(uniq),
                ))
            if has_transect:
                for t, row in zip(times, series):
                    profiles.append(pd.DataFrame(dict(
                        date=t.date(), site=name, variable=var,
                        distance_km=np.round(dist, 3), value=row, is_sea=is_sea)))
        t_agg += time.perf_counter() - t0
        ds.close()
        if not quiet:
            print(f"  {name:<16} {len(times):3d} days · box "
                  f"{box[1] - box[0]:.2f}x{box[3] - box[2]:.2f} deg")

    return summary, profiles, (t_down, t_agg)


def run(sensor, period):
    cfg = SENSORS[sensor]
    last_day = record_end(sensor)
    start, end = period_range(period, last_day)
    if start > last_day:
        print(f"\n=== {sensor} · {period}: starts after the record ends, skipped")
        return None
    tag = str(period).replace("-", "")
    print(f"\n=== {sensor} · {period} ({start} -> {end}) · {cfg['label']}")

    summary, profiles, (t_down, t_agg) = collect(sensor, start, end)
    if not summary:
        print("  nothing collected")
        return None

    DATA.mkdir(exist_ok=True)
    df = pd.DataFrame(summary)

    # Slim the CSV: the sensor is already in the filename, n_sea and n_pixels are
    # per-site constants repeated on every row, n_valid is recoverable from
    # valid_frac, and a row with no valid measure says nothing its absence
    # doesn't. Together that is about half the file.
    out_csv = DATA / f"chl_transect_{sensor}_{tag}.csv"
    measures = ["at_station", "transect_p80", "transect_mean", "transect_max"]
    slim = df.loc[~df[measures].isna().all(axis=1),
                  ["date", "site", "variable", "stream"] + measures + ["valid_frac"]]
    slim.to_csv(out_csv, index=False, float_format="%.4g")

    out_pq = None
    if profiles:
        prof = pd.concat(profiles, ignore_index=True)
        out_pq = DATA / f"chl_profile_{sensor}_{tag}.parquet"
        prof.to_parquet(out_pq, index=False, compression="zstd")

    print(f"  download {t_down:6.1f}s · aggregate {t_agg:5.1f}s · "
          f"{len(slim):,} summary rows"
          + (f" · {len(prof):,} profile rows" if out_pq else ""))
    print(f"  wrote {out_csv.name}"
          + (f" ({out_csv.stat().st_size / 1e3:.0f} kB)" if out_csv.exists() else "")
          + (f" and {out_pq.name} ({out_pq.stat().st_size / 1e6:.1f} MB)"
             if out_pq else ""))
    return dict(sensor=sensor, period=period, download_s=t_down, aggregate_s=t_agg,
                rows=len(df))


if __name__ == "__main__":
    argv = sys.argv[1:]
    wanted = [a for a in argv if a in SENSORS] or list(SENSORS)
    periods = [a for a in argv if a not in SENSORS] or YEARS
    stats = []
    t_all = time.perf_counter()
    for sensor in wanted:
        for period in periods:
            r = run(sensor, period)
            if r:
                stats.append(r)
    print(f"\ntotal wall clock {time.perf_counter() - t_all:.1f}s")
    if stats:
        s = pd.DataFrame(stats)
        print(s.to_string(index=False))
        print(f"\nper chunk: download {s.download_s.mean():.0f}s, "
              f"aggregate {s.aggregate_s.mean():.0f}s")
