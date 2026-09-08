"""
Refresh the chlorophyll transect CSV for the current year.

The reprocessed ocean-colour stream runs about a week behind, so re-fetching the
current year picks up whatever has been added since the last run. The remaining
days are then filled from the near-real-time stream, which lags by one day. Both
are tagged in a "stream" column, and reprocessed rows replace near-real-time
ones as soon as they exist - measured over their overlap the two agree to better
than 0.5%, so the join is not visible in the series.

Early in January the previous year is refreshed too, because its final days only
appear in the archive after the new year has started.

RUN:
    python chl_update.py [sensor ...]        # default: multi1km

    Needs COPERNICUSMARINE_SERVICE_USERNAME and COPERNICUSMARINE_SERVICE_PASSWORD
    in the environment (or ~/.copernicusmarine credentials).

OUTPUT:
    ../data/chl_transect_<sensor>_<year>.csv     rewritten in place
    ../data/chl_profile_<sensor>_<year>.parquet
"""

import datetime as dt
import os
import pathlib
import sys

import pandas as pd

from chl_transects import DATA, SENSORS, collect, record_end, run

MEASURES = ["at_station", "transect_p80", "transect_mean", "transect_max"]
KEY = ["date", "site", "variable"]


def fill_tail(sensor, year, nrt_sensor, prior=None):
    """Top the year's CSV up with near-real-time rows for days MY has not reached.

    Reprocessed rows always win. Near-real-time rows already in the file are
    kept even once they age out of the rolling window, so a run that is missed
    for longer than the window does not leave a permanent hole.
    """
    csv = DATA / f"chl_transect_{sensor}_{year}.csv"
    if not csv.exists():
        return 0
    have = pd.read_csv(csv)
    if "stream" not in have:
        have["stream"] = "my"
    # run() has just rewritten the file with reprocessed rows only, so the
    # near-real-time rows from earlier runs live in `prior` and are folded back
    # in here - otherwise a missed week would leave a permanent hole.
    if prior is not None and "stream" in prior:
        have = pd.concat([have, prior[prior["stream"] == "nrt"]], ignore_index=True)

    my_end = have.loc[have["stream"] == "my", "date"].max()
    nrt_end = record_end(nrt_sensor)
    start = (dt.date.fromisoformat(my_end) + dt.timedelta(days=1)).isoformat()
    if start > nrt_end:
        print(f"  near-real-time adds nothing (reprocessed already at {my_end})")
        return 0

    print(f"  topping up {start} -> {nrt_end} from {SENSORS[nrt_sensor]['label']}")
    summary, _, _ = collect(nrt_sensor, start, nrt_end, quiet=True)
    if not summary:
        return 0
    fresh = pd.DataFrame(summary)
    fresh["date"] = fresh["date"].astype(str)
    fresh = fresh.loc[~fresh[MEASURES].isna().all(axis=1),
                      KEY + ["stream"] + MEASURES + ["valid_frac"]]

    # my beats nrt; among equals the newly fetched row wins
    both = pd.concat([fresh, have], ignore_index=True)
    both["rank"] = (both["stream"] != "my").astype(int)
    both = (both.sort_values(["rank"], kind="stable")
                .drop_duplicates(subset=KEY, keep="first")
                .drop(columns="rank")
                .sort_values(["site", "date"]))
    added = len(both) - len(have)
    both.to_csv(csv, index=False, float_format="%.4g")
    return added

# Within this many days of New Year, also refresh the year just ended.
CATCHUP_DAYS = 40
DEFAULT_SENSORS = ["multi1km"]


def rows_in(sensor, year):
    f = DATA / f"chl_transect_{sensor}_{year}.csv"
    if not f.exists():
        return 0, None
    d = pd.read_csv(f)
    return len(d), d["date"].max()


def main(sensors):
    if not (os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME")
            and os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")) \
            and not (pathlib.Path.home() / ".copernicusmarine"
                     / ".copernicusmarine-credentials").exists():
        raise SystemExit(
            "No Copernicus Marine credentials found. Set "
            "COPERNICUSMARINE_SERVICE_USERNAME / COPERNICUSMARINE_SERVICE_PASSWORD.")

    today = dt.datetime.now(dt.UTC).date()
    years = [today.year]
    if today.timetuple().tm_yday <= CATCHUP_DAYS:
        years.append(today.year - 1)          # December lands after New Year

    changed = False
    for sensor in sensors:
        end = record_end(sensor)
        print(f"{sensor}: record currently ends {end}")
        for year in years:
            before, last_before = rows_in(sensor, year)
            csv = DATA / f"chl_transect_{sensor}_{year}.csv"
            prior = pd.read_csv(csv) if csv.exists() else None
            result = run(sensor, year)
            if result is None:
                continue
            nrt = SENSORS[sensor].get("nrt_twin", "nrt1km" if sensor == "multi1km" else None)
            if nrt and year == today.year:
                fill_tail(sensor, year, nrt, prior=prior)
            after, last_after = rows_in(sensor, year)
            if after != before or last_after != last_before:
                changed = True
                print(f"  {year}: {before:,} -> {after:,} rows, "
                      f"last day {last_before} -> {last_after}")
            else:
                print(f"  {year}: unchanged ({after:,} rows, to {last_after})")

    print("\nchanged" if changed else "\nnothing new")
    return 0


if __name__ == "__main__":
    wanted = [a for a in sys.argv[1:] if a in SENSORS] or DEFAULT_SENSORS
    sys.exit(main(wanted))
