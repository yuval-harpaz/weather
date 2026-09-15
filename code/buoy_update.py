"""
Daily summary of the Sdot Yam buoy, from the MKMRS dashboard API.

The buoy reports every 20 minutes. This keeps a daily median, minimum and
maximum of four measures, plus the two numbers needed to judge a day: how many
samples it rests on, and what fraction of the time the buoy was actually in the
water.

That last one matters more than it sounds. About 30% of the record so far was
recorded while the buoy was out of the water - capsized, recovered or grounded -
and the instruments go on reporting: the temperature swings between 8 and 40 C
as the sonde sits in the sun, while the optical channels pin at their blank
(chlorophyll 0.10, phycoerythrin -1.00) and look like a quiet sea. The ADCP's
own pressure channel gives it away, reading exactly 0.000 m when the instrument
is not submerged, so that is what every sample is filtered on. A separate marker,
-100000, means the value was never received at all.

The endpoint is not in this file. Put it in your profile instead:

    echo 'export BUOY_API="https://.../api/devices/<id>/parameters"' >> ~/.profile

RUN:
    python buoy_update.py            # fetch what is new, rewrite the daily table
    python buoy_update.py --rebuild  # re-fetch the whole record from scratch
    python buoy_update.py --dry-run  # report, write nothing

OUTPUT (in ../data):
    buoy_sdot_yam.csv    one row per day
"""

import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import numpy as np
import pandas as pd

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
OUT = DATA / "buoy_sdot_yam.csv"

# Named to sit beside the satellite columns the pages already read: `sst` is the
# water temperature and `chl` is the chlorophyll a chart would put at_station.
MEASURES = {
    "sst": 117387,          # Water Temperature, C
    "chl": 117391,          # Chlorophyll, ug/L
    "bga_pe": 117393,       # BGA-PE (phycoerythrin), ug/L
    "wave_hs": 117417,      # Hs Wave Height, m
}
DEPTH_PARAM = 117437        # ADCP pressure depth - the validity signal
RECORD_START = dt.date(2025, 3, 1)
WINDOW = 90                 # the API refuses a wider span
NO_DATA = -1000             # anything below this is the -100000 marker
SUBMERGED_M = 0.05          # deeper than this means the instrument is in the sea
# Depth is sampled on its own schedule, so each reading is matched to the
# nearest depth sample rather than assumed to share a timestamp.
MATCH_TOLERANCE = pd.Timedelta(minutes=30)


def api_base():
    url = os.environ.get("BUOY_API")
    if not url:
        raise SystemExit(
            "BUOY_API is not set. Add it to your profile:\n"
            "  echo 'export BUOY_API=\"https://.../api/devices/<id>/parameters\"'"
            " >> ~/.profile")
    return url.rstrip("/")


def get(url, tries=4):
    delay = 2.0
    for attempt in range(1, tries + 1):
        try:
            return json.loads(urllib.request.urlopen(url, timeout=180).read())
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


def fetch(pid, start, end):
    """Every sample for one parameter between two dates.

    Two limits apply at once: a request may span 90 days, and a response holds
    at most 5,000 records. The second bites on the faster channels and is only
    visible through `info.more`, so the inner loop keeps asking from the last
    timestamp it received until the API says there is no more.
    """
    base, rows, cur = api_base(), [], start
    while cur <= end:
        stop = min(cur + dt.timedelta(days=WINDOW - 1), end)
        frm = f"{cur} 00:00:00"
        while True:
            url = (f"{base}/{pid}/data?from={urllib.parse.quote(frm)}"
                   f"&to={urllib.parse.quote(f'{stop} 23:59:59')}")
            payload = get(url)
            part = payload.get("data", [])
            rows.extend(part)
            if not payload.get("info", {}).get("more") or not part:
                break
            frm = part[-1]["timestamp"]
            time.sleep(0.8)
        cur = stop + dt.timedelta(days=1)
        time.sleep(0.8)
    if not rows:
        return pd.Series(dtype=float)
    f = pd.DataFrame(rows)
    f["t"] = pd.to_datetime(f["timestamp"], errors="coerce")
    f["v"] = pd.to_numeric(f["value"], errors="coerce")
    s = (f.dropna(subset=["t", "v"]).drop_duplicates("t")
          .set_index("t")["v"].sort_index())
    return s.mask(s < NO_DATA)          # never received, not a reading


def submerged_mask(index, depth):
    """True where the buoy was in the water at that moment."""
    if depth.empty:
        return pd.Series(False, index=index)
    near = depth.reindex(index, method="nearest", tolerance=MATCH_TOLERANCE)
    return (near > SUBMERGED_M).fillna(False)


def summarise(raw, depth):
    """Daily median, minimum and maximum of the submerged samples."""
    frames, counts = [], {}
    for name, s in raw.items():
        if s.empty:
            continue
        wet = submerged_mask(s.index, depth)
        good = s.where(wet)
        g = good.groupby(good.index.normalize())
        frames.append(pd.DataFrame({name: g.median(), f"{name}_min": g.min(),
                                    f"{name}_max": g.max()}))
        counts[name] = g.count()
    day = pd.concat(frames, axis=1)
    day["samples"] = pd.concat(counts.values(), axis=1).max(axis=1)
    if not depth.empty:
        sub = (depth > SUBMERGED_M)
        day["submerged"] = sub.groupby(sub.index.normalize()).mean()
    day.index = pd.to_datetime(day.index).date
    day.index.name = "date"          # after the replacement, or the name is lost
    return day.round(3)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true",
                    help="re-fetch the whole record instead of only what is new")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    args = ap.parse_args(argv)

    have = None
    if OUT.exists() and not args.rebuild:
        have = pd.read_csv(OUT, parse_dates=["date"])
        have["date"] = have["date"].dt.date
        have = have.set_index("date")
    # Re-fetch the last two stored days as well: the newest ones can still be
    # filling in when a run happens mid-day.
    start = (min(have.index[-2:]) if have is not None and len(have) >= 2
             else RECORD_START)
    end = dt.date.today()
    print(f"fetching {start} -> {end}"
          + ("  (full rebuild)" if have is None else ""))

    depth = fetch(DEPTH_PARAM, start, end)
    print(f"  depth {len(depth):,} samples, "
          f"{100 * (depth > SUBMERGED_M).mean():.0f}% submerged")
    raw = {}
    for name, pid in MEASURES.items():
        raw[name] = fetch(pid, start, end)
        print(f"  {name:<8}{len(raw[name]):>8,} samples")
    if not any(len(s) for s in raw.values()):
        raise SystemExit("nothing came back - is BUOY_API right?")

    fresh = summarise(raw, depth)
    day = fresh if have is None else pd.concat(
        [have[~have.index.isin(fresh.index)], fresh]).sort_index()

    kept = int(day["samples"].fillna(0).gt(0).sum())
    print(f"\n{len(day):,} days, {day.index.min()} -> {day.index.max()}, "
          f"{kept:,} with usable samples")
    if "submerged" in day:
        out_of_water = int((day["submerged"].fillna(0) < 0.2).sum())
        print(f"  {out_of_water} days the buoy was out of the water")

    if args.dry_run:
        before = len(have) if have is not None else 0
        print(f"  dry run: {before:,} rows on disk would become {len(day):,}")
        return 0

    DATA.mkdir(exist_ok=True)
    day.to_csv(OUT)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.0f} kB)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
