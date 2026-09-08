"""
Refresh the chlorophyll transect CSV for the current year.

The reprocessed ocean-colour stream runs about a week behind, so re-fetching the
current year picks up whatever has been added since the last run. Only the
reprocessed stream is used - mixing in near-real-time data would put a
processing seam in the middle of the series.

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

from chl_transects import DATA, SENSORS, record_end, run

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
            result = run(sensor, year)
            if result is None:
                continue
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
