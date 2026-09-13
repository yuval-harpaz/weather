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
    python grid_update.py            # fetch what changed, re-derive, write
    python grid_update.py --rebuild  # re-download all 55 months from scratch
    python grid_update.py --dry-run  # report what would change, write nothing

OUTPUT (in ../data):
    grid_peaks.csv      one row per day, all months in one file
    grid_sources.csv    which monthly file each month came from, and when

Only months that are new, republished under a new link, or among the newest two
are downloaded; everything else is carried over from the table on disk. A normal
week is one request, for the index page. Use --rebuild after changing how a
month is parsed, or to check the older months for corrections.
"""

import argparse
import csv
import datetime as dt
import io
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request

import numpy as np
import pandas as pd

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
OUT = DATA / "grid_peaks.csv"
# What was fetched last time, so a run can tell a new month from a known one.
SOURCES = DATA / "grid_sources.csv"

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
# A reserve can also fall away without demand rising, and that is a different
# animal: the load was ordinary and the generation simply was not there. Those
# days do not reach the tight line and no weather explains them, so they would
# pass unnoticed - 28 February 2026 sat at 12.9% with a peak 207 MW BELOW its
# own fortnight, while the days either side of it held 30-38%.
SUPPLY_PCT = 15.0
# The newest months are re-fetched every run even when their link has not
# changed, because a late correction to last month is plausible and would
# otherwise never be noticed. Older months are treated as settled; use
# --rebuild to check them.
REFRESH_MONTHS = 2
# The columns a month contributes. Everything else in the output is derived
# from these and recomputed locally on every run.
RAW_KEEP = ["date", "peak_hour", "capacity_mw", "peak_mw", "renewables_mw",
            "storage_mw", "reserve_mw", "demand_response"]


def fetch(url, timeout=60, tries=4):
    """Fetch with backoff.

    Fifty-five files in a row is enough for the server to start refusing, and a
    refusal used to cost a month of history: a whole run once lost twelve months
    this way. Transient failures are retried, and the status code is reported so
    a permanent one is still recognisable as permanent.
    """
    delay = 2.0
    for attempt in range(1, tries + 1):
        try:
            return urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=timeout).read()
        except urllib.error.HTTPError as exc:
            if exc.code in (400, 401, 403, 404) or attempt == tries:
                raise
            print(f"    HTTP {exc.code}, retrying in {delay:.0f}s "
                  f"({attempt}/{tries - 1})")
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == tries:
                raise
            print(f"    {type(exc).__name__}, retrying in {delay:.0f}s "
                  f"({attempt}/{tries - 1})")
        time.sleep(delay)
        delay *= 2
    raise SystemExit("unreachable")


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


MONTH_RE = re.compile(r"peak_hour_(\d{2})-?(\d{2})-?(\d{4})\.csv")


def file_month(name):
    """'peak_hour_31-05-2022.csv' and 'peak_hour_31012026.csv' -> '2022-05'."""
    m = MONTH_RE.search(name)
    return f"{m.group(3)}-{m.group(2)}" if m else None


def read_sources():
    """{filename: url} as last fetched, or empty on the first incremental run."""
    if not SOURCES.exists():
        return {}
    d = pd.read_csv(SOURCES)
    return dict(zip(d["file"], d["url"]))


def read_existing():
    """The raw columns of the table already on disk, or None."""
    if not OUT.exists():
        return None
    d = pd.read_csv(OUT)
    if "date" not in d:
        return None
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date
    d["demand_response"] = (d["demand_response"].astype(str).str.lower() == "true")
    missing = [c for c in RAW_KEEP if c not in d.columns]
    if missing:
        print(f"  existing table lacks {missing}; a full rebuild is needed")
        return None
    return d.dropna(subset=["date"])[RAW_KEEP]


def collect(force_all=False):
    """Fetch what has changed and fold it into the table already on disk.

    Rebuilding all fifty-five months every week was fifty-five requests for
    data that changes once a month, and asking for them in a burst is what got
    twelve of them refused at once - a run that then published a table missing
    a year. Reading the index costs one request; usually nothing else is needed.
    """
    links = month_links()
    known = read_sources()
    have = None if force_all else read_existing()
    by_file = {l.split("/")[-1]: l for l in links}
    months = {name: file_month(name) for name in by_file}
    recent = sorted(m for m in months.values() if m)[-REFRESH_MONTHS:]

    # A table built before this file existed is trusted for the months it
    # already covers, rather than re-downloading all of them to prove it.
    if have is not None and not known:
        covered = {str(d)[:7] for d in have["date"]}
        if all(m in covered for m in months.values()):
            known = dict(by_file)
            print("  adopting the existing table as the starting point")

    wanted = []
    for name, link in by_file.items():
        if have is None:
            wanted.append(name)
        elif name not in known:
            wanted.append(name)                 # a month that is new to us
        elif known[name] != link:
            wanted.append(name)                 # republished under a new id
        elif months[name] in recent:
            wanted.append(name)                 # late corrections land here
    wanted.sort()

    print(f"index lists {len(links)} monthly files; "
          f"{len(wanted)} to fetch{' (full rebuild)' if have is None else ''}")

    frames, failed = [], []
    for i, name in enumerate(wanted, 1):
        try:
            frames.append(parse_month(fetch(SITE + by_file[name])))
        except Exception as exc:
            code = getattr(exc, "code", "")
            print(f"  {name}: {type(exc).__name__} {code}, gave up")
            failed.append(name)
            continue
        time.sleep(0.6)                     # their server, their pace
        if len(wanted) > 12 and i % 12 == 0:
            print(f"  {i}/{len(wanted)}")

    # A month that fails must not shorten the table. Starting from what is
    # already on disk means a failure can only mean "nothing new today", but a
    # full rebuild has no such floor, so it is refused outright.
    if failed:
        raise SystemExit(f"{len(failed)} of {len(wanted)} months failed to "
                         f"download ({', '.join(failed[:3])}); refusing to "
                         f"write from a partial set")
    if have is None and not frames:
        raise SystemExit("nothing downloaded - has the page moved?")

    fresh = (pd.concat(frames, ignore_index=True)[RAW_KEEP]
             if frames else pd.DataFrame(columns=RAW_KEEP))
    if have is None:
        df = fresh
    else:
        touched = {str(d)[:7] for d in fresh["date"]} if len(fresh) else set()
        keep = have[~have["date"].astype(str).str[:7].isin(touched)]
        df = pd.concat([keep, fresh], ignore_index=True)
        print(f"  {len(touched)} month(s) refreshed, "
              f"{len(keep):,} rows carried over unchanged")

    df = (df.drop_duplicates(subset="date", keep="last")
            .sort_values("date").reset_index(drop=True))
    # Only record what was actually proven good by this run.
    fetched = dt.date.today().isoformat()
    rows = [{"file": n, "url": by_file[n], "month": months[n],
             "fetched": fetched if n in wanted else ""} for n in sorted(by_file)]
    pd.DataFrame(rows).to_csv(SOURCES, index=False)
    return df


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

    events, notes, kinds, vals, whos, pcts, oks = [], [], [], [], [], [], []
    for _, r in df.iterrows():
        day, pct, peak = r["date"], r["reserve_pct"], r["peak_mw"]
        tight = pd.notna(pct) and pct <= TIGHT_PCT
        big = pd.notna(peak) and peak > BIG_PEAK_MW
        spike = bool(r["peak_spike"])
        # A demand-management call only counts as an event if the day was
        # actually under pressure - either the reserve was not comfortable, or
        # the peak itself was large.
        dr = bool(r["demand_response"]) and (pd.isna(pct) or pct <= EASY_PCT or big)
        # Low reserve on a day when demand was not high: the squeeze came from
        # the supply side, so no temperature will account for it.
        supply = (not tight and pd.notna(pct) and pct <= SUPPLY_PCT
                  and pd.notna(r["peak_vs_local"]) and r["peak_vs_local"] <= 0)
        parts = ([("demand response" if dr else None)]
                 + [("tight reserve" if tight else None)]
                 + [("peak spike" if spike else None)]
                 + [("low reserve, demand ordinary" if supply else None)])
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
        if supply:
            gap = abs(r["peak_vs_local"])
            how = (f"{gap:,.0f} MW below" if gap >= 1 else "level with")
            note = (f"{event} — {pct:.1f}% reserve on a peak {how} the days "
                    f"around it, so the shortfall was in supply, not demand")
            events.append(event); notes.append(note); kinds.append("")
            vals.append(np.nan); whos.append(""); pcts.append(np.nan)
            oks.append(False)          # supply-side: weather is not the reason
            continue
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
        oks.append(bool(event) and explained)
        notes.append(note)
        kinds.append(kind or "")
        vals.append(value if value is not None else np.nan)
        whos.append(station or "")
        pcts.append(round(score, 4) if score is not None else np.nan)

    df["event"] = events
    df["explained"] = oks
    df["temp_station"] = whos
    df["temp_kind"] = kinds
    df["temp_c"] = vals
    df["temp_rank"] = pcts
    df["note"] = notes
    return df


def collapse_runs(df):
    """Keep one flag per run of consecutive flagged days.

    A cold snap or a heatwave lasts days, and flagging each of them says the
    same thing several times over - 10 to 12 June 2026 were three consecutive
    spikes of one event. The run is represented by its most extreme day: the
    biggest peak where the run contains a spike, otherwise the lowest reserve.
    The others are kept in the table with their numbers intact and simply stop
    being flagged, so nothing is lost, only the repetition.
    """
    flagged = df.index[df["event"] != ""].tolist()
    if not flagged:
        df["suppressed"] = False
        df["run_days"] = 0
        return df
    runs, cur = [], [flagged[0]]
    for i in flagged[1:]:
        prev = cur[-1]
        if (df.at[i, "date"] - df.at[prev, "date"]).days == 1:
            cur.append(i)
        else:
            runs.append(cur); cur = [i]
    runs.append(cur)

    suppressed = pd.Series(False, index=df.index)
    run_days = pd.Series(0, index=df.index)
    for run in runs:
        run_days.loc[run] = len(run)
        if len(run) == 1:
            continue
        if df.loc[run, "peak_spike"].any():
            keep = df.loc[run, "peak_mw"].idxmax()
            how = "highest peak"
        else:
            keep = df.loc[run, "reserve_pct"].idxmin()
            how = "lowest reserve"
        for i in run:
            if i != keep:
                suppressed[i] = True
        df.at[keep, "note"] += (f" · the {how} of {len(run)} consecutive "
                                f"flagged days, "
                                f"{df.at[run[0], 'date']} to {df.at[run[-1], 'date']}")
    df["suppressed"] = suppressed
    df["run_days"] = run_days
    df.loc[suppressed, "event"] = ""
    df.loc[suppressed, "explained"] = False
    return df


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    ap.add_argument("--rebuild", action="store_true",
                    help="re-download all months instead of only what changed")
    args = ap.parse_args(argv)

    df = collect(force_all=args.rebuild)
    df["reserve_pct"] = (100 * df["reserve_mw"] / df["peak_mw"]).round(2)
    df["peak_prom_mw"] = prominence(df["peak_mw"].to_numpy())
    local = df["peak_mw"].rolling(SPIKE_WINDOW * 2 + 1, center=True,
                                  min_periods=7).median()
    df["peak_vs_local"] = (df["peak_mw"] - local).round(0)
    df["peak_spike"] = ((df["peak_mw"] > BIG_PEAK_MW)
                        & (df["peak_prom_mw"] >= SPIKE_PROM_MW)).fillna(False)
    df = annotate(df)
    df = collapse_runs(df)

    before = len(pd.read_csv(OUT)) if OUT.exists() else 0
    gap = pd.date_range(df["date"].min(), df["date"].max()).difference(
        pd.to_datetime(df["date"]))
    print(f"{len(df):,} days, {df['date'].min()} -> {df['date'].max()}, "
          f"{len(gap)} missing")
    # Only the ones demoted for being comfortable - not the ones dropped as
    # repeats inside a run, which are a different thing entirely.
    quiet = int((df["demand_response"] & (df["event"] == "")
                 & ~df["suppressed"]).sum())
    print(f"  demand response on {int(df['demand_response'].sum())} days "
          f"({quiet} of them on a comfortable reserve, shown but not flagged)")
    print(f"  peak spikes {int(df['peak_spike'].sum())}, "
          f"{int((df['event'] != '').sum())} days flagged after collapsing "
          f"{int(df['suppressed'].sum())} repeats within runs, "
          f"{int(df['explained'].sum())} explained by temperature")

    if args.dry_run:
        print(f"  dry run: {before:,} rows on disk would become {len(df):,}")
        return 0

    DATA.mkdir(exist_ok=True)
    out = df[["date", "peak_hour", "peak_mw", "capacity_mw", "reserve_mw",
              "reserve_pct", "peak_prom_mw", "peak_vs_local", "renewables_mw",
              "storage_mw", "demand_response", "peak_spike", "event",
              "explained", "suppressed", "run_days",
              "temp_station", "temp_kind", "temp_c", "temp_rank", "note"]]
    out.to_csv(OUT, index=False)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.0f} kB), "
          f"{before:,} -> {len(out):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
