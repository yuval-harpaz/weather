"""
Daily peak-hour reserve margin of the Israeli electricity grid.

Noga, the system operator, publishes a monthly CSV of the daily peak under its
reporting obligation: the peak itself, the available capacity behind it, the
reserve left at that moment, and whether the demand-management contracts had to
be invoked to get through it. That last column is the published trace of large
consumers being shed - desalination plants among them - which is the mechanism
the State Comptroller described in its November 2024 report on desalination,
where early February 2023 saw four plants shed during a cold snap.

The files live at
https://www.noga-iso.co.il/obligation-to-report/daily-demand-peaks/
one per month, back to January 2022. Their URLs carry a random hash so they
cannot be constructed - the index page has to be read for the links. They are
plain CSV, openly downloadable; the live demand curve on the same site is behind
a CAPTCHA and is deliberately not used here.

Every tight day is then checked against the station temperatures this repository
already collects, because the reserve is squeezed by weather: over 2022 - 2026
the days that fell below a tenth of the peak in reserve averaged a national
minimum of -0.6 C, against +8.9 C on every other day. A day is called explained
when the responsible station sat in the tail of its own seasonal distribution.

RUN:
    python grid_update.py            # refresh, rebuild, re-annotate
    python grid_update.py --dry-run  # report what would change, write nothing

OUTPUT (in ../data):
    grid_peaks.csv    one row per day, all months in one file
"""

import argparse
import csv
import datetime as dt
import io
import pathlib
import re
import sys
import time
import urllib.request

import numpy as np
import pandas as pd

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
OUT = DATA / "grid_peaks.csv"

INDEX = "https://www.noga-iso.co.il/obligation-to-report/daily-demand-peaks/"
SITE = "https://www.noga-iso.co.il"
UA = {"User-Agent": "Mozilla/5.0 (compatible; yuval-harpaz/weather)"}
LINK_RE = re.compile(r"/media/[a-z0-9]+/peak_hour_[0-9-]+\.csv")

# The published columns, in order, renamed on the way in. The source headers are
# Hebrew and have changed spacing between years, so position is the stable key.
COLUMNS = ["date", "peak_hour", "capacity_forecast_mw", "capacity_mw",
           "peak_forecast_mw", "peak_mw", "renewables_mw", "storage_mw",
           "reserve_forecast_mw", "reserve_mw", "demand_response"]

# Which station answers for a given season, and which way the temperature has to
# go to count. Bet Dagan is the coastal reference station and Jerusalem Centre
# the inland one; between them they cover where the load actually is.
STATIONS = ["BET DAGAN", "JERUSALEM CENTRE"]
WINTER = {11, 12, 1, 2, 3}
SUMMER = {5, 6, 7, 8, 9}
# A day counts as explained when the station sat in this tail of its own season.
TAIL = 0.10
# Reserve at or below this share of the peak is tight enough to look at even
# when the demand-management contracts were not invoked.
TIGHT_PCT = 10.0
# Above this, the reserve is comfortable: a demand-management call on such a day
# is not a shortage - 8 March 2023 was invoked on a 9.4 GW peak with 30% spare -
# so it is kept in the data and shown, but not counted among the flagged days.
EASY_PCT = 20.0
# ...unless the peak itself was large. Roughly a quarter of all days clear this,
# so it is a floor for "worth noticing", never a flag on its own.
BIG_PEAK_MW = 12000
# A spike is judged against its own neighbours rather than against the same date
# in other years, because demand is growing fast: 12 June was 11.5 GW in 2022
# and 15.6 GW in 2026, so a seasonal comparison would flag most of a recent
# summer. The measure is topographic prominence - how far the day stands above
# the higher of the two valleys either side of it - which is what makes a peak
# look like a peak. On that measure 12 June 2026 is the most prominent day in
# the whole record, 5,278 MW clear, half again as much as anything else.
SPIKE_WINDOW = 7          # days either side to search for the flanking valleys
SPIKE_PROM_MW = 3000


def fetch(url, timeout=60):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=timeout).read()


def month_links():
    """Every monthly CSV the index page offers, oldest first."""
    html = fetch(INDEX).decode("utf-8", "ignore")
    return sorted(set(LINK_RE.findall(html)))


def decode(raw):
    """Their exports mix UTF-8 with Hebrew ANSI, so the encoding is sniffed."""
    for enc in ("utf-8-sig", "cp1255"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "ignore")


def parse_month(raw):
    rows = [r for r in csv.reader(io.StringIO(decode(raw))) if r and any(r)]
    if len(rows) < 2:
        return pd.DataFrame()
    body = [r for r in rows[1:] if len(r) >= len(COLUMNS)]
    df = pd.DataFrame([r[:len(COLUMNS)] for r in body], columns=COLUMNS)
    # Dates appear as both 01/02/2023 and 01-02-2023 depending on the month.
    df["date"] = pd.to_datetime(df["date"].str.strip(), dayfirst=True,
                                errors="coerce").dt.date
    for c in COLUMNS[2:-1]:
        df[c] = pd.to_numeric(df[c].str.strip().replace("", np.nan),
                              errors="coerce")
    df["demand_response"] = (df["demand_response"].str.strip() == "כן")
    return df.dropna(subset=["date"])


def collect():
    """Download every published month and stack them into one frame."""
    links = month_links()
    print(f"index lists {len(links)} monthly files")
    frames = []
    for i, link in enumerate(links, 1):
        try:
            frames.append(parse_month(fetch(SITE + link)))
        except Exception as exc:
            print(f"  {link.split('/')[-1]}: {type(exc).__name__}, skipped")
            continue
        time.sleep(0.2)                     # their server, their pace
        if i % 12 == 0:
            print(f"  {i}/{len(links)}")
    if not frames:
        raise SystemExit("nothing downloaded - has the page moved?")
    df = pd.concat(frames, ignore_index=True)
    return (df.drop_duplicates(subset="date", keep="last")
              .sort_values("date").reset_index(drop=True))


def prominence(peaks):
    """How far each day stands above the higher valley on either side of it.

    The plain height of a peak says little when the baseline is drifting - a
    16 GW August day is unremarkable in an August of 16 GW days. Prominence
    asks how far you would have to descend before you could climb to somewhere
    higher, which is the shape a reader actually sees on the chart.
    """
    out = np.full(len(peaks), np.nan)
    for i in range(len(peaks)):
        left = peaks[max(0, i - SPIKE_WINDOW):i]
        right = peaks[i + 1:i + 1 + SPIKE_WINDOW]
        if not len(left) or not len(right):
            continue
        floor = max(np.nanmin(left), np.nanmin(right))
        out[i] = peaks[i] - floor
    return np.round(out)


def station_daily(kind, years, how):
    """Daily extreme per station, from the hourly files already in the repo."""
    out = {}
    for year in years:
        f = DATA / f"temp_{kind}_{year}.csv"
        if not f.exists():
            continue
        d = pd.read_csv(f, parse_dates=["datetime"])
        have = [s for s in STATIONS if s in d.columns]
        if not have:
            continue
        d = d.set_index("datetime")[have]
        agg = getattr(d.resample("D"), how)()
        for day, row in agg.iterrows():
            out[day.date()] = {s: row[s] for s in have if pd.notna(row[s])}
    return out


def annotate(df):
    """Say, for each tight day, whether the weather accounts for it.

    Winter is judged on the daily minimum and summer on the daily maximum; the
    shoulder months are judged on whichever of the two is more extreme for its
    own season. "Extreme" means the tail of that station's own distribution
    across the years covered here, so the test travels if the climate shifts.
    """
    years = sorted({d.year for d in df["date"]})
    mins = station_daily("min", years, "min")
    maxs = station_daily("max", years, "max")

    # The reference distributions: one per station per season.
    cold = {s: np.array([v[s] for d, v in mins.items()
                         if d.month in WINTER and s in v]) for s in STATIONS}
    hot = {s: np.array([v[s] for d, v in maxs.items()
                        if d.month in SUMMER and s in v]) for s in STATIONS}

    events, notes, kinds, vals, whos, pcts = [], [], [], [], [], []
    for _, r in df.iterrows():
        day, pct, peak = r["date"], r["reserve_pct"], r["peak_mw"]
        tight = pd.notna(pct) and pct <= TIGHT_PCT
        big = pd.notna(peak) and peak > BIG_PEAK_MW
        spike = bool(r["peak_spike"])
        # A demand-management call only counts as an event if the day was
        # actually under pressure - either the reserve was not comfortable, or
        # the peak itself was large.
        dr = bool(r["demand_response"]) and (pd.isna(pct) or pct <= EASY_PCT or big)
        parts = ([("demand response" if dr else None)]
                 + [("tight reserve" if tight else None)]
                 + [("peak spike" if spike else None)])
        event = ", ".join([x for x in parts if x])
        best = (None, None, None, None)     # station, kind, value, percentile
        if event:
            # Winter asks how cold it got, summer how hot; a shoulder month is
            # allowed to answer either way and the stronger signal wins.
            tests = []
            if day.month in WINTER:
                tests.append(("min", mins.get(day, {}), cold, True))
            if day.month in SUMMER:
                tests.append(("max", maxs.get(day, {}), hot, False))
            if not tests:                   # April and October
                tests = [("min", mins.get(day, {}), cold, True),
                         ("max", maxs.get(day, {}), hot, False)]
            for kind, today, ref, low in tests:
                for s in STATIONS:
                    if s not in today or not len(ref.get(s, [])):
                        continue
                    p = float((ref[s] < today[s]).mean())      # share below
                    score = p if low else 1 - p                # 0 = most extreme
                    if best[3] is None or score < best[3]:
                        best = (s, kind, today[s], score)
        station, kind, value, score = best
        explained = score is not None and score <= TAIL
        lead = event
        if r["peak_spike"] and pd.notna(r["peak_prom_mw"]):
            lead = (f"{event} of {r['peak_mw']:,.0f} MW, standing "
                    f"{r['peak_prom_mw']:,.0f} MW above the days around it")
        if not event:
            note = ""
        elif station is None:
            note = f"{lead} — no station reading for this day"
        else:
            where = station.title()
            side = "colder" if kind == "min" else "hotter"
            note = (f"{lead} — {where} {value:.1f} °C, "
                    f"{side} than {(1 - score) * 100:.0f}% of "
                    f"{'winter' if kind == 'min' else 'summer'} days"
                    + ("" if explained else "; not an extreme day"))
        events.append(event)
        notes.append(note)
        kinds.append(kind or "")
        vals.append(value if value is not None else np.nan)
        whos.append(station or "")
        pcts.append(round(score, 4) if score is not None else np.nan)

    df["event"] = events
    df["explained"] = [bool(e) and bool(n) and "not an extreme" not in n
                       for e, n in zip(events, notes)]
    df["temp_station"] = whos
    df["temp_kind"] = kinds
    df["temp_c"] = vals
    df["temp_rank"] = pcts
    df["note"] = notes
    return df


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    args = ap.parse_args(argv)

    df = collect()
    df["reserve_pct"] = (100 * df["reserve_mw"] / df["peak_mw"]).round(2)
    df["peak_prom_mw"] = prominence(df["peak_mw"].to_numpy())
    df["peak_spike"] = ((df["peak_mw"] > BIG_PEAK_MW)
                        & (df["peak_prom_mw"] >= SPIKE_PROM_MW)).fillna(False)
    df = annotate(df)

    before = len(pd.read_csv(OUT)) if OUT.exists() else 0
    gap = pd.date_range(df["date"].min(), df["date"].max()).difference(
        pd.to_datetime(df["date"]))
    print(f"{len(df):,} days, {df['date'].min()} -> {df['date'].max()}, "
          f"{len(gap)} missing")
    quiet = int((df["demand_response"] & (df["event"] == "")).sum())
    print(f"  demand response on {int(df['demand_response'].sum())} days "
          f"({quiet} of them on a comfortable reserve, shown but not flagged)")
    print(f"  peak spikes {int(df['peak_spike'].sum())}, "
          f"{int((df['event'] != '').sum())} days flagged, "
          f"{int(df['explained'].sum())} explained by temperature")

    if args.dry_run:
        print(f"  dry run: {before:,} rows on disk would become {len(df):,}")
        return 0

    DATA.mkdir(exist_ok=True)
    out = df[["date", "peak_hour", "peak_mw", "capacity_mw", "reserve_mw",
              "reserve_pct", "peak_prom_mw", "renewables_mw", "storage_mw",
              "demand_response", "peak_spike", "event", "explained",
              "temp_station", "temp_kind", "temp_c", "temp_rank", "note"]]
    out.to_csv(OUT, index=False)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.0f} kB), "
          f"{before:,} -> {len(out):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
