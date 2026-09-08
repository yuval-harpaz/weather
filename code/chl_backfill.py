"""
One-off maintenance: add a newly defined site to the history, and rebuild every
yearly CSV from the saved profiles so the MIN_VALID rule applies to data that
was collected before the rule existed.

Only the new site is downloaded. Everything else is recomputed from the parquet
profiles already on disk, which hold every transect pixel - so no re-download.

RUN:
    python chl_backfill.py [site ...]        # default: sites missing from the CSVs

OUTPUT:
    ../data/chl_transect_multi1km_<year>.csv   rebuilt
    ../data/chl_profile_multi1km_<year>.parquet  new site appended
"""

import sys
import time

import numpy as np
import pandas as pd

from chl_transects import (DATA, MIN_VALID, SITES, collect, period_range,
                           record_end, CHL_OFFSHORE_KM)

SENSOR = "multi1km"
YEARS = range(2015, 2027)
MEASURES = ["at_station", "transect_p80", "transect_mean", "transect_max"]
COLS = ["date", "site", "variable", "stream"] + MEASURES + ["valid_frac"]


def summarise(prof):
    """Rebuild the daily summary from a profile table, applying MIN_VALID."""
    rows = []
    for (date, site, var), g in prof.groupby(["date", "site", "variable"],
                                             sort=False):
        sea = g[g["is_sea"]]
        vals = sea["value"].values
        valid = np.isfinite(vals)
        frac = float(valid.mean()) if len(vals) else 0.0
        station = g.loc[np.isclose(g["distance_km"], CHL_OFFSHORE_KM), "value"]
        enough = valid.any() and frac >= MIN_VALID
        rows.append(dict(
            date=str(date), site=site, variable=var, stream="my",
            at_station=float(station.iloc[0]) if len(station) else np.nan,
            transect_p80=np.nanpercentile(vals[valid], 80) if enough else np.nan,
            transect_mean=np.nanmean(vals[valid]) if enough else np.nan,
            transect_max=np.nanmax(vals[valid]) if enough else np.nan,
            valid_frac=round(frac, 4)))
    return pd.DataFrame(rows)


def main(wanted):
    missing = wanted or []
    if not missing:
        have = pd.read_csv(DATA / f"chl_transect_{SENSOR}_2025.csv")["site"].unique()
        missing = [s for s in SITES if s not in have]
    print(f"sites to add: {missing or 'none'}")

    t0 = time.perf_counter()
    for year in YEARS:
        csv = DATA / f"chl_transect_{SENSOR}_{year}.csv"
        pq = DATA / f"chl_profile_{SENSOR}_{year}.parquet"
        if not csv.exists() or not pq.exists():
            print(f"{year}: no files, skipped")
            continue

        old = pd.read_csv(csv)
        if "stream" not in old:
            old["stream"] = "my"
        prof = pd.read_parquet(pq)
        prof["date"] = prof["date"].astype(str)

        # download only the new site, only for this year
        for site in missing:
            if not SITES[site].get("transect"):
                continue
            start, end = period_range(year, record_end(SENSOR))
            if start > end:
                continue
            summary, profiles, _ = collect(SENSOR, start, end, quiet=True,
                                           sites=[site])
            if profiles:
                add = pd.concat(profiles, ignore_index=True)
                add["date"] = add["date"].astype(str)
                prof = pd.concat([prof[prof["site"] != site], add],
                                 ignore_index=True)
                prof.to_parquet(pq, index=False, compression="zstd")

        rebuilt = summarise(prof)

        # rows the profiles do not carry: the point-only site, and any
        # near-real-time tail, which never had profiles written for it
        keep = old[(~old["site"].isin(rebuilt["site"].unique()))
                   | (old["stream"] != "my")].copy()
        for c in MEASURES[1:]:                       # apply the rule to those too
            keep.loc[keep["valid_frac"] < MIN_VALID, c] = np.nan

        out = pd.concat([rebuilt, keep], ignore_index=True)
        out = out.loc[~out[MEASURES].isna().all(axis=1), COLS]
        out = out.sort_values(["site", "date"])
        out.to_csv(csv, index=False, float_format="%.4g")

        n_new = (out["site"].isin(missing)).sum()
        dropped = len(old) - len(out) + n_new
        print(f"{year}: {len(old):,} -> {len(out):,} rows "
              f"(+{n_new:,} new site, -{dropped:,} below {MIN_VALID:.0%} coverage)")

    print(f"\n{time.perf_counter() - t0:.0f}s")


if __name__ == "__main__":
    main([a for a in sys.argv[1:] if a in SITES])
