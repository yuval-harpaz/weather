"""
Daily electricity generation in Israel by source - gas, coal, liquid fuel, solar...

Noga publishes, under the same reporting obligation as the daily peaks, a monthly
CSV of gross generation per unit per half hour ("stations-nopv" feed, ~28 MB a
month). Despite the name it includes solar: one row per half hour carries
"Total Estimated PV Generation", Noga's estimate of all PV including rooftop and
distribution-connected plants. The separate "stations-pv" feed lists those
distribution plants one by one and is NOT added here - it is already inside that
total and would be counted twice.

Values are MWh per half hour (twice that is the average MW). They are summed per
day and source and written as GWh; the raw files are not kept.

The listing page is
https://www.noga-iso.co.il/obligation-to-report-new/?feed=stations-nopv

RUN:
    python grid_mix_update.py            # fetch new or partial months only
    python grid_mix_update.py --rebuild  # re-download every month
    python grid_mix_update.py --dry-run  # report what would change, write nothing

OUTPUT (in ../data):
    grid_mix.csv   date, one GWh column per source, total_gwh, slots
                   (slots = half hours present that day; 48 normally, 46/50
                   on daylight-saving change days)

A month already on disk is fetched again only when it looks partial: a missing
day, or a day with fewer than 46 half hours.
"""

import argparse
import csv
import html as html_lib
import io
import re
import sys
import time

import pandas as pd

from grid_update import DATA, SITE, decode, fetch

OUT = DATA / "grid_mix.csv"
INDEX = "https://www.noga-iso.co.il/obligation-to-report-new/?feed="
LINK_RE = (r"/umbraco/api/ObligationFiles/Download\?feed={feed}"
           r"&(?:amp;)?file=prod_{stem}_web_[0-9.]+\.csv")
MONTH_RE = re.compile(r"_(\d{2})\.(\d{2})\.(\d{4})\.csv")
MIN_SLOTS = 46

# Noga's fuel labels -> output columns, in stacking order (baseload at the bottom).
# Diesel (סולר) and heavy fuel oil (מזוט) are both "liquid fuel".
FUELS = {
    "פחם": "coal",
    "גז": "gas",
    "סולר": "liquid",
    "מזוט": "liquid",
    "BIO-GAS": "biogas",
    "PSP": "pumped_storage",
    "WIND": "wind",
    "THERMO-SOLAR": "solar_thermal",
    "PV": "solar_pv",
}
SOURCES = list(dict.fromkeys(FUELS.values())) + ["other"]
# Coal units co-firing fuel oil are labelled with the mix, e.g.
# "75% פחם + 25% מזוט"; their output is split between coal and liquid by it.
BLEND_RE = re.compile(r"(\d+)%\s*פחם\s*\+\s*(\d+)%\s*מזוט")
# About a tenth of generation has a blank fuel label: mostly the steam turbines
# of combined-cycle plants, which run on the gas turbines' exhaust, plus a coal
# unit or two. Those are filled from the primary fuel in the monthly unit
# register (feed "units"), whose labels are spelled differently.
REGISTER_FUELS = {
    "פחם": "coal",
    "גז טבעי": "gas",
    "סולר": "liquid",
    "מזוט": "liquid",
    "קרוסין": "liquid",
    "ביו-גז": "biogas",
    "חשמל בשאיבה/מים בייצור": "pumped_storage",
    "רוח": "wind",
    "שמש": "solar_pv",
}


def month_links(feed="stations-nopv", stem="stations_nopv"):
    page = fetch(INDEX + feed).decode("utf-8", "ignore")
    pattern = re.compile(LINK_RE.format(feed=feed, stem=stem))
    links = {html_lib.unescape(l) for l in pattern.findall(page)}
    out = {}
    for l in links:
        m = MONTH_RE.search(l)
        if m:
            out[f"{m.group(3)}-{m.group(2)}"] = l
    return dict(sorted(out.items()))


def parse_register(raw):
    """Unit register -> {unit name: output column}, from its primary fuel."""
    rows = list(csv.reader(io.StringIO(decode(raw))))
    return {r[1].strip(): REGISTER_FUELS.get(r[7].strip(), "other")
            for r in rows[1:] if len(r) > 7}


def parse_month(raw, register=None):
    """Half-hourly per-unit rows -> one row per day, GWh per source.

    Columns are taken by position (date, time, unit, MWh, fuel); the headers are
    Hebrew and one of the later ones was renamed between years.
    """
    rows = list(csv.reader(io.StringIO(decode(raw))))
    df = pd.DataFrame([r[:5] for r in rows[1:] if len(r) >= 5],
                      columns=["date", "time", "unit", "mwh", "fuel"])
    df["date"] = pd.to_datetime(df["date"].str.strip(), format="%d/%m/%Y",
                                errors="coerce").dt.date
    df["mwh"] = pd.to_numeric(df["mwh"].str.strip(), errors="coerce")
    df = df.dropna(subset=["date", "mwh"])
    fuel = df["fuel"].str.strip()
    blend = fuel.str.extract(BLEND_RE).astype(float)
    is_blend = blend[0].notna()
    if is_blend.any():
        b = df[is_blend]
        coal_share = blend.loc[is_blend, 0] / (blend.loc[is_blend, 0]
                                               + blend.loc[is_blend, 1])
        oil = b.assign(mwh=b["mwh"] * (1 - coal_share), fuel="מזוט")
        df.loc[is_blend, "mwh"] = b["mwh"] * coal_share
        df.loc[is_blend, "fuel"] = "פחם"
        df = pd.concat([df, oil], ignore_index=True)
        fuel = df["fuel"].str.strip()
    unknown = sorted(set(fuel) - set(FUELS) - {""})
    if unknown:
        print(f"    unknown fuel label(s) {unknown}, counted as 'other'")
    df["source"] = fuel.map(FUELS)
    blank = fuel.eq("")
    unit = df["unit"].str.strip()
    # 1) the unit register; 2) whatever the other units of the same site burn.
    if register:
        df.loc[blank, "source"] = unit[blank].map(register)
    site = unit.str.replace(r"\s*יחידה\s*\d+$", "", regex=True)
    labelled = df[df["source"].notna() & ~blank]
    by_site = (labelled.groupby([site[labelled.index], "source"])["mwh"].sum()
                       .groupby(level=0).idxmax().map(lambda t: t[1]))
    still = df["source"].isna() & blank
    df.loc[still, "source"] = site[still].map(by_site)
    left = df["source"].isna() & blank
    if df.loc[left, "mwh"].sum() >= 50:
        print(f"    {df.loc[left, 'mwh'].sum() / 1000:.1f} GWh with no fuel "
              f"label or match, counted as 'other'")
    df["source"] = df["source"].fillna("other")
    wide = (df.pivot_table(index="date", columns="source", values="mwh",
                           aggfunc="sum")
              .reindex(columns=SOURCES).fillna(0.0) / 1000)
    wide["total_gwh"] = wide.sum(axis=1)
    wide = wide.round(3)
    wide["slots"] = df.groupby("date")["time"].nunique()
    return wide.reset_index()


def partial_months(have):
    if have is None or not len(have):
        return set()
    d = have.assign(month=have["date"].astype(str).str[:7])
    ok = d[d["slots"] >= MIN_SLOTS]
    days = ok.groupby("month")["date"].nunique()
    return {m for m in d["month"].unique()
            if days.get(m, 0) < pd.Period(m).days_in_month}


def read_existing():
    if not OUT.exists():
        return None
    d = pd.read_csv(OUT)
    d["date"] = pd.to_datetime(d["date"]).dt.date
    return d


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--rebuild", action="store_true")
    args = ap.parse_args(argv)

    links = month_links()
    registers = month_links("units", "units")
    have = None if args.rebuild else read_existing()
    covered = set() if have is None else {str(d)[:7] for d in have["date"]}
    partial = partial_months(have)
    wanted = [m for m in links if m not in covered or m in partial]
    print(f"index lists {len(links)} monthly files; {len(wanted)} to fetch"
          f"{' (full rebuild)' if have is None else ''}")

    frames, failed = [], []
    for i, m in enumerate(wanted, 1):
        try:
            register = None
            if m in registers:
                try:
                    register = parse_register(fetch(SITE + registers[m]))
                except Exception as exc:
                    print(f"    {m} unit register: {type(exc).__name__}, "
                          f"falling back to same-site fuel")
            frames.append(parse_month(fetch(SITE + links[m], timeout=180),
                                      register))
            print(f"  {m} ok")
        except Exception as exc:
            print(f"  {m}: {type(exc).__name__} {getattr(exc, 'code', '')}, gave up")
            failed.append(m)
        time.sleep(1.0)                     # big files; go gently

    if have is None and failed:
        raise SystemExit(f"{len(failed)} months failed on a full build; "
                         f"refusing to write a table with holes")
    if not frames:
        print("nothing new" + (f"; {len(failed)} failed" if failed else ""))
        return 1 if failed else 0

    fresh = pd.concat(frames, ignore_index=True)
    if have is None:
        df = fresh
    else:
        touched = {str(d)[:7] for d in fresh["date"]}
        keep = have[~have["date"].astype(str).str[:7].isin(touched)]
        df = pd.concat([keep, fresh], ignore_index=True)
    df = (df.drop_duplicates(subset="date", keep="last")
            .sort_values("date").reset_index(drop=True))
    df = df[["date"] + SOURCES + ["total_gwh", "slots"]]

    gap = pd.date_range(df["date"].min(), df["date"].max()).difference(
        pd.to_datetime(df["date"]))
    short = int((df["slots"] < MIN_SLOTS).sum())
    print(f"{len(df):,} days, {df['date'].min()} -> {df['date'].max()}, "
          f"{len(gap)} missing, {short} short")
    if args.dry_run:
        return 0
    df.to_csv(OUT, index=False)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.0f} kB)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
