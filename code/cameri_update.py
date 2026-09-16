"""
Daily sea temperature from the CAMERI wave buoys at Haifa, Ashdod and Eilat.

NOT CLEARED FOR PUBLICATION. The numbers belong to CAMERI (the Coastal and
Marine Engineering Research Institute at the Technion), reached through the
Grafana instance their public page embeds. Reading it is one thing; committing
it to a repository that serves a web page is republishing, and that needs their
say-so first. Until it is given, `data/cameri_buoys.csv` is in .gitignore -
delete that line when the answer comes back, and add the workflow step then.

  Israel Ports "Waves" page -> www.cameri-eng.com/physical-oceanography-
  measurements-data/ -> adva.cameri-eng.com, dashboard `cameri-wave-buoys-hae`.

The buoys report a two-hour average, about twelve values a day, and the table
behind the dashboard keeps only the last three or four days - asking for ten
years returns the same three days. So there is no archive to backfill: the
record only exists from the first run onwards, and a run must happen at least
every third day or a gap opens that nothing can fill afterwards.

What a row is: the mean of one UTC day's two-hour averages. UTC rather than
local time because the point of collecting this is to sit it beside the
satellite pixel for the same station, and those are UTC days.

Haifa reports nothing at the moment. That is not an error and not a reason to
stop - its rows will start appearing on their own, so it is fetched like the
others and simply contributes no days until it does.

RUN:
    python cameri_update.py              # fetch, merge, rewrite the table
    python cameri_update.py --dry-run    # report, write nothing
    python cameri_update.py --days 3     # how far back to ask (default 4)

OUTPUT (in ../data):
    cameri_buoys.csv     one row per city per day
"""

import argparse
import datetime as dt
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request

import pandas as pd

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
OUT = DATA / "cameri_buoys.csv"

GRAFANA = "https://adva.cameri-eng.com/api/ds/query?orgId=5"
DATASOURCE = {"type": "grafana-postgresql-datasource",
              "uid": "d4fb9d12-3057-41b7-9ce2-36c7320d8a58"}
# id -> city, from the dashboard's own `location_id` variable query.
LOCATIONS = {1: "Haifa", 2: "Ashdod", 3: "Eilat"}
# The three measures the dashboard's own table panel selects, from the table its
# four panels all read. Nothing here goes beyond what that panel already shows.
SQL = ('SELECT middle_time AS "time", avg_temp, avg_hs, avg_tp '
       'FROM backend_buoysmsr_2h_avg '
       'WHERE location_id = {loc} '
       'AND middle_time BETWEEN $__timeFrom() AND $__timeTo() '
       'ORDER BY middle_time;')
LOOKBACK_DAYS = 4          # a shade more than the table retains
COLUMNS = ["date", "city", "sst", "sst_min", "sst_max", "wave_hs", "wave_tp",
           "samples"]


def post(body, tries=4):
    data = json.dumps(body).encode()
    req = urllib.request.Request(GRAFANA, data=data, method="POST",
                                 headers={"Content-Type": "application/json",
                                          "Accept": "application/json"})
    delay = 2.0
    for attempt in range(1, tries + 1):
        try:
            return json.loads(urllib.request.urlopen(req, timeout=60).read())
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 401, 403, 404) or attempt == tries:
                raise
            print(f"      HTTP {exc.code}, retry in {delay:.0f}s")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == tries:
                raise
            print(f"      {type(exc).__name__}, retry in {delay:.0f}s")
        time.sleep(delay)
        delay *= 2


def fetch(loc, days):
    """Every two-hour average for one buoy over the last `days` days."""
    payload = post({
        "queries": [{"refId": "A", "datasource": DATASOURCE, "format": "table",
                     "rawQuery": True, "rawSql": SQL.format(loc=loc)}],
        "from": f"now-{days}d", "to": "now",
    })
    res = payload.get("results", {}).get("A", {})
    if res.get("error"):
        raise RuntimeError(f"location {loc}: {res['error']}")
    frames = res.get("frames") or []
    if not frames:
        return pd.DataFrame()
    frame = frames[0]
    names = [f["name"] for f in frame["schema"]["fields"]]
    cols = frame["data"]["values"]
    if not cols or not cols[0]:
        return pd.DataFrame()
    f = pd.DataFrame(dict(zip(names, cols)))
    # Grafana hands time back as epoch milliseconds, UTC.
    f["t"] = pd.to_datetime(f["time"], unit="ms", utc=True)
    return f


def summarise(raw, city):
    """One row per UTC day: the mean of that day's two-hour averages."""
    if raw.empty:
        return pd.DataFrame(columns=COLUMNS)
    f = raw.copy()
    f["date"] = f["t"].dt.date
    g = f.groupby("date")
    day = pd.DataFrame({
        "sst": g["avg_temp"].mean(),
        "sst_min": g["avg_temp"].min(),
        "sst_max": g["avg_temp"].max(),
        "wave_hs": g["avg_hs"].mean(),
        "wave_tp": g["avg_tp"].mean(),
        # Counted on the temperature column, so a day's mean and its sample
        # count always describe the same thing.
        "samples": g["avg_temp"].count(),
    }).round(3).reset_index()
    # ISO strings, not date objects: the stored table comes back from CSV as
    # text, and a column holding both cannot be sorted or compared. ISO dates
    # sort correctly as text anyway.
    day["date"] = day["date"].astype(str)
    day.insert(1, "city", city)
    return day[COLUMNS]


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=LOOKBACK_DAYS,
                    help="how far back to ask for (the table holds ~4 days)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    args = ap.parse_args(argv)

    have = None
    if OUT.exists():
        have = pd.read_csv(OUT, dtype={"date": str})

    fresh, failed = [], []
    for loc, city in LOCATIONS.items():
        try:
            raw = fetch(loc, args.days)
        except Exception as exc:                     # noqa: BLE001 - reported below
            failed.append(city)
            print(f"  {city:<8} FAILED: {exc}")
            continue
        day = summarise(raw, city)
        fresh.append(day)
        if day.empty:
            print(f"  {city:<8} no data published")
        else:
            print(f"  {city:<8} {len(raw):>3} readings -> {len(day)} days, "
                  f"{day['date'].min()} to {day['date'].max()}, "
                  f"{day['sst'].iloc[-1]:.2f} °C on the last")
        time.sleep(0.5)

    # A partial fetch would quietly drop whichever city failed, and because the
    # source keeps only four days those rows could never be recovered. Better to
    # write nothing and run again.
    if failed:
        raise SystemExit(f"{len(failed)} of {len(LOCATIONS)} buoys failed "
                         f"({', '.join(failed)}); refusing to write a partial set")

    new = pd.concat(fresh, ignore_index=True) if fresh else pd.DataFrame(columns=COLUMNS)
    if have is not None and len(have):
        # A day first seen part-way through is re-summarised on the next run, so
        # the fresh copy of a (date, city) always wins over the stored one.
        keys = set(zip(new["date"].astype(str), new["city"]))
        keep = have[~have.apply(
            lambda r: (str(r["date"]), r["city"]) in keys, axis=1)]
        table = pd.concat([keep, new], ignore_index=True)
    else:
        table = new
    table = table.sort_values(["date", "city"]).reset_index(drop=True)

    print(f"\n{len(table):,} rows, "
          f"{table['date'].min() if len(table) else '-'} to "
          f"{table['date'].max() if len(table) else '-'}")
    for city in LOCATIONS.values():
        n = int((table["city"] == city).sum()) if len(table) else 0
        print(f"  {city:<8} {n:>4} days")

    if args.dry_run:
        print(f"  dry run: {0 if have is None else len(have):,} rows on disk "
              f"would become {len(table):,}")
        return 0

    DATA.mkdir(exist_ok=True)
    table.to_csv(OUT, index=False)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.1f} kB)")
    print("  reminder: not cleared for publication - see the module docstring")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
