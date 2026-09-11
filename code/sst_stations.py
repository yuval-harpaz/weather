"""
Daily sea surface temperature at each coastal station.

Two analyses of the same water, kept apart. The reprocessed one is 5 km and runs
from 1982, ending about a month back; the near-real-time one is 1 km and runs
from 2008 to today. They disagree by under a tenth of a degree, but splicing
them into a single series would still put a step at whatever date the join
happened to fall on, so each stream gets its own file and the page decides which
to draw.

Both per-station files stop at the archive's end, and one small trailing file
carries every station's days after that. That is what keeps the repository
small: the per-station files are thousands of rows but are rewritten only when
the archive advances, roughly monthly, while the file rewritten every day holds
a few hundred rows. It also removes the reconciliation logic the chlorophyll
updater needs - the trailing file starts the day after the per-station files
end, so nothing has to decide which copy of a day wins.

Values are foundation SST - the temperature with the day's solar warming removed
- so there is one number per day and no daily maximum to take. The hourly
subskin product measures the diurnal cycle instead, but it starts in 2019 and
cannot carry the 1991-2020 baseline.

RUN:
    python sst_stations.py backfill     # whole 1982-> record, ~1 minute
    python sst_stations.py              # daily update

    Needs COPERNICUSMARINE_SERVICE_USERNAME and COPERNICUSMARINE_SERVICE_PASSWORD
    in the environment (or ~/.copernicusmarine credentials).

OUTPUT (in ../data):
    sst_rep_<Station>.csv    date,sst   5 km reprocessed, 1982 -> archive end
    sst_nrt_<Station>.csv    date,sst   1 km near real time, 2008 -> archive end
    sst_nrt.csv              date,station,sst   1 km, the days after that
    sst_stations.csv         station,lat,lon,order   geography for the map
"""

import datetime as dt
import pathlib
import sys

import numpy as np
import pandas as pd
import copernicusmarine as cm

from chl_transects import credentials
from coast_points import CHL_POINTS, SST_EXTRA_POINTS

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"

# 0.05 deg, 1982 -> about a month back. The baseline and every drawn year.
REP = "cmems_SST_MED_SST_L4_REP_OBSERVATIONS_010_021"
# 1/100 deg, 2008 -> today. Chosen over the 1/16 deg twin because its offset
# from the reprocessed record is smaller and far steadier along this coast:
# worst station 0.13 C against the coarser grid's 0.23 C at Port Said.
NRT = "SST_MED_SST_L4_NRT_OBSERVATIONS_010_004_c_V2"

REP_START = "1982-01-01"
# The near-real-time record starts here, so before 2008 the archive is the only
# thing there is and the page has no choice to offer.
NRT_START = "2008-01-01"
# Enough to survive a long outage without ever growing without bound. The window
# only has to reach back to wherever the archive currently ends.
NRT_MAX_DAYS = 150
BOX_DEG = 0.10
KELVIN = 273.15

# North to south along the coast, then west along Sinai and the delta - the same
# order the chlorophyll page uses, with the two northern stations on the front.
ORDER = ["Nahariya_IL", "Haifa_IL", "Hadera_IL", "Tel_Aviv_IL", "Ashdod_IL",
         "Ashkelon_IL", "Gaza_City", "Rafah_Gaza", "El_Arish_EG", "Bardawil_off",
         "Port_Said_EG", "Damietta_mouth", "Rosetta_mouth", "Open_sea_ref"]

CREDS = credentials()


def stations():
    """{name: (lat, lon)} in coast order, from the shared geography."""
    out = {}
    for name in ORDER:
        if name in SST_EXTRA_POINTS:
            out[name] = SST_EXTRA_POINTS[name]
        elif name in CHL_POINTS:
            out[name] = CHL_POINTS[name]
        else:
            raise SystemExit(f"{name} is in neither chl_points.json nor "
                             f"SST_EXTRA_POINTS - run shore_angle.py first?")
    return out


_END_CACHE = {}


def record_end(dataset_id):
    """Last day this dataset actually offers, from the CMEMS catalogue.

    Read at run time rather than hardcoded: the near-real-time record moves
    every day and the archive every month, and a frozen constant would quietly
    stop an automated updater the moment either of them advanced.
    """
    if dataset_id in _END_CACHE:
        return _END_CACHE[dataset_id]
    cat = cm.describe(dataset_id=dataset_id, disable_progress_bar=True)
    ds = cat.products[0].datasets[0]
    svc = [sv for v in ds.versions for pt in v.parts for sv in pt.services
           if "arco-time" in str(sv.service_name)][0]
    co = [c for c in svc.variables[0].coordinates if c.coordinate_id == "time"][0]
    end = dt.datetime.fromtimestamp(co.maximum_value / 1000,
                                    dt.UTC).strftime("%Y-%m-%d")
    _END_CACHE[dataset_id] = end
    return end


def series(dataset_id, lat, lon, start, end):
    """Daily degrees C at the sea pixel nearest the station, indexed by date."""
    if start > end:
        return pd.Series(dtype=float)
    ds = cm.open_dataset(
        dataset_id=dataset_id, variables=["analysed_sst"],
        minimum_longitude=lon - BOX_DEG, maximum_longitude=lon + BOX_DEG,
        minimum_latitude=lat - BOX_DEG, maximum_latitude=lat + BOX_DEG,
        start_datetime=f"{start}T00:00:00", end_datetime=f"{end}T00:00:00",
        **CREDS)
    ds = ds.load()
    lats, lons = ds.latitude.values, ds.longitude.values
    cube = ds.analysed_sst.values
    i = int(np.abs(lats - lat).argmin())
    j = int(np.abs(lons - lon).argmin())
    # An L4 analysis is NaN on land and nowhere else, so a station whose nearest
    # pixel is NaN has landed on the coast mask rather than under a cloud, and
    # the fix is to step to the nearest wet pixel instead of dropping the day.
    if not np.isfinite(cube[:, i, j]).any():
        sea = np.isfinite(cube[0])
        if not sea.any():
            ds.close()
            raise SystemExit(f"no sea pixel within {BOX_DEG} deg of "
                             f"{lat:.3f},{lon:.3f} in {dataset_id}")
        yy, xx = np.nonzero(sea)
        d2 = (lats[yy] - lat) ** 2 + ((lons[xx] - lon) * np.cos(np.radians(lat))) ** 2
        k = int(d2.argmin())
        i, j = int(yy[k]), int(xx[k])
    out = pd.Series(cube[:, i, j] - KELVIN,
                    index=[t.date().isoformat()
                           for t in pd.to_datetime(ds.time.values)])
    ds.close()
    return out.round(2).dropna()


# The two records the page can draw, kept in separate files on purpose: mixing a
# 5 km and a 1 km analysis into one series would put a step of a tenth of a
# degree in the middle of it at whatever date the join happened to fall.
STREAMS = {
    "rep": dict(dataset=REP, start=REP_START, label="5 km reprocessed, 1982 ->"),
    "nrt": dict(dataset=NRT, start=NRT_START, label="1 km near real time, 2008 ->"),
}


def stream_file(stream, name):
    return DATA / f"sst_{stream}_{name}.csv"


def write_geography(pts):
    (pd.DataFrame([(n, la, lo, k) for k, (n, (la, lo)) in enumerate(pts.items())],
                  columns=["station", "lat", "lon", "order"])
     .to_csv(DATA / "sst_stations.csv", index=False))


def backfill():
    """Fetch both records for every station, from scratch."""
    pts = stations()
    end = record_end(REP)
    print(f"reprocessed record ends {end}; both per-station files stop there, "
          f"and sst_nrt.csv carries the days after it")
    for stream in STREAMS:
        cfg = STREAMS[stream]
        print(f"\n{cfg['label']}")
        for name, (la, lo) in pts.items():
            s = series(cfg["dataset"], la, lo, cfg["start"], end)
            if s.empty:
                print(f"  {name:<16} nothing returned")
                continue
            s.rename_axis("date").rename("sst").to_csv(stream_file(stream, name))
            print(f"  {name:<16} {len(s):6,} days  {s.index[0]} -> {s.index[-1]}"
                  f"  mean {s.mean():.2f} C")
    write_geography(pts)
    return 0


def extend(pts, stream, end):
    """Append whatever each record has added since the files were last written.

    Both streams stop at the archive's end on purpose. The near-real-time one
    could run right up to today, but then the same days would live in a
    per-station file and in the trailing file at once, and the page would have
    to decide which copy wins. Cutting both at the same boundary means the
    trailing file is simply the continuation.
    """
    cfg = STREAMS[stream]
    added = 0
    for name, (la, lo) in pts.items():
        f = stream_file(stream, name)
        if not f.exists():
            print(f"  {name}: no {stream} file yet - run `backfill` first")
            continue
        have = pd.read_csv(f, index_col="date")["sst"]
        start = (dt.date.fromisoformat(have.index[-1])
                 + dt.timedelta(days=1)).isoformat()
        if start > end:
            continue
        fresh = series(cfg["dataset"], la, lo, start, end)
        if fresh.empty:
            continue
        both = pd.concat([have, fresh])
        both = both[~both.index.duplicated(keep="last")]
        both.rename_axis("date").rename("sst").to_csv(f)
        added += len(fresh)
        print(f"  {name:<16} {stream} +{len(fresh)} days -> {both.index[-1]}")
    return added


def update_nrt(pts, rep_end):
    """Rewrite the trailing file: everything the archive has not reached yet."""
    end = record_end(NRT)
    start = (dt.date.fromisoformat(rep_end) + dt.timedelta(days=1)).isoformat()
    floor = (dt.date.fromisoformat(end)
             - dt.timedelta(days=NRT_MAX_DAYS)).isoformat()
    start = max(start, floor)
    if start > end:
        print(f"  near-real-time adds nothing (archive already at {rep_end})")
        (DATA / "sst_nrt.csv").write_text("date,station,sst\n")
        return 0, end
    rows = []
    for name, (la, lo) in pts.items():
        for date, v in series(NRT, la, lo, start, end).items():
            rows.append((date, name, v))
    df = pd.DataFrame(rows, columns=["date", "station", "sst"])
    df.sort_values(["date", "station"]).to_csv(DATA / "sst_nrt.csv", index=False)
    print(f"  near-real-time {start} -> {end}, {len(df):,} rows "
          f"across {df.station.nunique()} stations")
    return len(df), end


def main(argv):
    DATA.mkdir(exist_ok=True)
    if argv and argv[0] == "backfill":
        return backfill()

    pts = stations()
    rep_end = record_end(REP)
    print(f"archive ends {rep_end}")
    for stream in STREAMS:
        extend(pts, stream, rep_end)
    n, nrt_end = update_nrt(pts, rep_end)
    write_geography(pts)
    print(f"latest day available {nrt_end}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
