"""
Daily summary of the Ashkelon mooring, from IOLR's SeaDataNet/ODV exports.

Built to sit beside data/buoy_sdot_yam.csv and summarised the same way: a daily
median with the day's minimum and maximum either side of it. Median rather than
mean, because a single spike in an optical channel should not move the day.

The exports are one file per instrument and period, and they overlap, so every
file is read and the samples pooled; a timestamp seen twice for the same channel
is kept once.

Two things in these files need handling before the numbers mean anything:

  Fill values. Absent readings are written as 999999, and the ADCP currents also
  use -32768. Left in, a tenth of the turbidity column reads as 999999 and the
  channel's mean comes out at 104,010 instead of 0.78.

  Quality flags. The newer exports carry real SeaDataNet codes - 1,783
  fluorescence samples are flagged bad - while the older ones are entirely 0,
  which means "no quality control was done" and must not be read as a failure.
  Codes 3, 4 and 9 are dropped; 0 is kept.

The Depth column is not a depth. The site's bottom is 26 m, yet the column holds
11, 12, 62 and 120, and the readings labelled 120 average 27.5 C with a maximum
of 31.2 - surface water, not 120 m, where the Levantine Sea sits near 16 C all
year. They are deployment labels, so everything is pooled. On the ADCP the same
column really is a profile, 40 bins half a metre apart from 4.0 m down to
23.5 m, and only the shallowest bin carrying a reading is used.

Every channel takes the shallowest sample available at each instant, and the
depth it came from is written out beside it, so a day summarised from a deeper
bin than usual is visible rather than silently mixed in.

RUN:
    python ashkelon_iolr.py [directory]      # default ~/Documents
    python ashkelon_iolr.py --dry-run

OUTPUT (in ../data):
    ashkelon_iolr.csv    one row per day
"""

import argparse
import glob
import os
import pathlib
import sys

import numpy as np
import pandas as pd

HERE = pathlib.Path(__file__).parent
DATA = HERE.parent / "data"
OUT = DATA / "ashkelon_iolr.csv"
DEFAULT_DIR = pathlib.Path.home() / "Documents"
PATTERN = "Ashkelon*.txt"

# Absent readings, by the value the exports use rather than by a blank cell.
FILL = {999999.0, -999999.0, -32768.0, -999.0, -9999.0}
# SeaDataNet L20: 0 no QC, 1 good, 2 probably good, 3 probably bad, 4 bad,
# 8 interpolated, 9 missing. Only the unambiguous failures go.
BAD_FLAGS = {3, 4, 9}

# ODV channel name -> the column it becomes here. Named for what was actually
# measured: WaveHightMax is the largest wave of the burst, not the significant
# wave height that `wave_hs` means at Sdot Yam, and the fluorescence channel is
# dimensionless - a chlorophyll proxy, not a chlorophyll concentration.
CHANNELS = {
    "WaterTemp [degC]":        "sst",
    "Fluorescence [Dmnless]":  "fluor",
    "Turbidity [NTU]":         "turb",
    "WaveHightMax [m]":        "wave_hmax",
    "WaveAvgPer ADWA [s]":     "wave_per",
}
# Written out exactly as the file gives it, but the label looks wrong: the
# median is 0.102 and the 95th percentile 0.378, which as cm/s would be a median
# current of one millimetre a second on an open coast. Read as m/s they become
# 10 cm/s and 38 cm/s, which is what this shelf actually does. Not converted
# here - a silent factor of a hundred is worse than a labelled doubt - so ask
# IOLR before using the magnitudes.
CURRENT_SPEED = "CurrentSpeed [cm/s]"
CURRENT_DIR = "CurrentDir [deg T]"
CHUNK = 500_000


def header_of(path):
    """Column names and the //-preamble, without reading the body."""
    pre, cols = [], None
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        for line in fh:
            line = line.rstrip("\r\n")
            if line.startswith("//"):
                pre.append(line)
            elif line.strip():
                cols = line.split("\t")
                break
    return pre, cols


def clean(values, flags):
    """Fill values and failed quality codes out, as NaN."""
    v = pd.to_numeric(values, errors="coerce").astype("float64")
    v[v.isin(FILL)] = np.nan
    if flags is not None:
        q = pd.to_numeric(flags, errors="coerce")
        v[q.isin(BAD_FLAGS)] = np.nan
    return v


def read_file(path):
    """Long-format samples from one export: time, channel, value."""
    pre, cols = header_of(path)
    if not cols:
        return None, pre
    time_col = next((c for c in cols if c.startswith("time_ISO8601")), None)
    if time_col is None:
        return None, pre
    # A QV column qualifies the one before it, so a column followed by one is a
    # measurement. Duplicate names are made unique by position, the way pandas
    # does, so the QV columns can be matched back to their values.
    uniq, seen = [], {}
    for c in cols:
        if c in seen:
            seen[c] += 1
            uniq.append(f"{c}.{seen[c]}")
        else:
            seen[c] = 0
            uniq.append(c)
    pairs = []
    for i, c in enumerate(cols):
        wanted = set(CHANNELS) | {CURRENT_SPEED, CURRENT_DIR}
        if i + 1 < len(cols) and cols[i + 1] == "QV:SEADATANET" and c in wanted:
            pairs.append((c, uniq[i], uniq[i + 1]))
    if not pairs:
        return None, pre
    depth_name = next((uniq[i] for i, c in enumerate(cols)
                       if c.startswith("Depth [")), None)
    time_name = uniq[cols.index(time_col)]
    want = [time_name] + ([depth_name] if depth_name else []) \
        + [n for _, n, _ in pairs] + [q for _, _, q in pairs]

    out = []
    reader = pd.read_csv(path, sep="\t", skiprows=len(pre), names=uniq,
                         header=0, usecols=want, chunksize=CHUNK,
                         dtype=str, engine="c", on_bad_lines="skip")
    for chunk in reader:
        t = pd.to_datetime(chunk[time_name], errors="coerce", format="ISO8601")
        keep = t.notna()
        if not keep.any():
            continue
        frame = {"t": t[keep]}
        if depth_name:
            frame["depth"] = pd.to_numeric(chunk.loc[keep, depth_name], errors="coerce")
        for raw, vname, qname in pairs:
            frame[raw] = clean(chunk.loc[keep, vname], chunk.loc[keep, qname])
        out.append(pd.DataFrame(frame))
    if not out:
        return None, pre
    return pd.concat(out, ignore_index=True), pre


def shallowest(df, cols):
    """One row per timestamp: the shallowest bin that actually has a reading.

    Not the shallowest bin outright - the shallowest with data. An ADCP's top
    bins drop out in low water and the CTD's depth label changes between
    deployments, so asking for "4 m" or "11 m" by name would leave holes that
    are really just a different bin carrying the measurement that day.
    """
    s = df[["t", "depth"] + cols].dropna(subset=cols)
    if s.empty:
        return None
    s = s.sort_values(["t", "depth"], kind="stable")
    return s.groupby("t", as_index=False).first()


def daily_scalar(df, raw, name):
    """Median, min and max per day, from the shallowest reading of each instant."""
    s = shallowest(df, [raw])
    if s is None:
        return None
    g = s.set_index("t")
    agg = g[raw].groupby(pd.Grouper(freq="D"))
    out = pd.DataFrame({name: agg.median(), f"{name}_min": agg.min(),
                        f"{name}_max": agg.max(), f"{name}_n": agg.count()})
    out[f"{name}_depth"] = g["depth"].groupby(pd.Grouper(freq="D")).median()
    return out


def daily_current(df):
    """Daily current from one fixed bin, as a vector.

    Not the shallowest available bin, which is what every other channel uses.
    The ADCP's top bins come and go: 4.0 m covers only 71% of the days and
    vanishes altogether from 2022, while 11.5 m is the shallowest bin present on
    every one of the 3,255 days. Following "shallowest available" would have
    quietly walked the series from 4 m to 11 m and back between deployments, and
    a current at 4 m is not the same measurement as a current at 11 m. One bin,
    chosen for coverage, is the honest series; the bin is printed and written out.

    Speed and bearing cannot be averaged on their own - the mean of 350 deg and
    10 deg is 180, the exact opposite - so each reading becomes east and north
    components, those are averaged, and the magnitude and bearing are read back
    off the result. Both are written out: cur_speed is the length of the mean
    vector, which is the net flow and goes to nearly zero when the day reverses,
    while cur_speed_mean is the average of the instantaneous speeds, which does
    not. They answer different questions and the pair is more honest than either.
    """
    if CURRENT_SPEED not in df or CURRENT_DIR not in df:
        return None, None
    s = df[["t", "depth", CURRENT_SPEED, CURRENT_DIR]].dropna()
    if s.empty:
        return None, None
    days = s["t"].dt.floor("D")
    cover = s.assign(day=days).groupby("depth")["day"].nunique()
    total = days.nunique()
    full = sorted(d for d, n in cover.items() if n == total)
    bin_m = full[0] if full else cover.idxmax()
    s = s[s["depth"] == bin_m]

    rad = np.radians(s[CURRENT_DIR].to_numpy())
    spd = s[CURRENT_SPEED].to_numpy()
    s = s.assign(u=spd * np.sin(rad), v=spd * np.cos(rad), spd=spd)
    g = s.set_index("t").groupby(pd.Grouper(freq="D"))
    day = g[["u", "v"]].mean()
    out = pd.DataFrame({
        "cur_east": day["u"],
        "cur_north": day["v"],
        "cur_speed": np.hypot(day["u"], day["v"]),
        "cur_speed_mean": g["spd"].mean(),
        "cur_speed_max": g["spd"].max(),
        "cur_dir": (np.degrees(np.arctan2(day["u"], day["v"])) + 360) % 360,
        "cur_n": g["spd"].count(),
    })
    # Only where the day actually has current, or a constant would be
    # written on every row of the merged table including empty ones.
    out["cur_depth"] = np.where(out["cur_n"] > 0, bin_m, np.nan)
    return out, bin_m


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("directory", nargs="?", default=str(DEFAULT_DIR),
                    help="where the IOLR exports are (default ~/Documents)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    paths = sorted(glob.glob(os.path.join(args.directory, PATTERN)))
    if not paths:
        raise SystemExit(f"no {PATTERN} in {args.directory}")

    frames, notes = [], []
    for p in paths:
        df, pre = read_file(p)
        name = os.path.basename(p)
        if df is None:
            print(f"  {name}: no usable channels, skipped")
            continue
        chans = [c for c in df.columns if c not in ("t", "depth")]
        print(f"  {name}: {len(df):,} rows, {', '.join(chans)}")
        notes.append(name)
        frames.append(df)

    if not frames:
        raise SystemExit("nothing readable")
    all_df = pd.concat(frames, ignore_index=True)

    # No row-level de-duplication here. Two of these exports are the same CTD
    # deployment split by channel, so they share every timestamp: dropping
    # duplicate (time, depth) pairs across files deleted the temperature rows as
    # "duplicates" of the turbidity ones and cut the record from 3,000 days to
    # 511. Where a channel genuinely does appear twice for one instant, the
    # per-timestamp median below collapses it harmlessly.

    # Two channels record "not measured" as a literal zero rather than leaving
    # the cell empty, and both are physically impossible as readings:
    #   wave_per is exactly 0 on every day from 2012 to 2021 and only carries
    #   real values from 2022 - a mean wave period of zero seconds is not a
    #   measurement, it is a channel that was not being recorded.
    #   turb and fluor are both exactly 0 on the same 159 days of 2020 and on no
    #   others. Two independent optical channels reading a clean zero together,
    #   on the same days, is an instrument that was off.
    # Taken one at a time a zero could be real, so only these patterns go.
    if "WaveAvgPer ADWA [s]" in all_df:
        all_df.loc[all_df["WaveAvgPer ADWA [s]"] == 0, "WaveAvgPer ADWA [s]"] = np.nan
    tu, fl = "Turbidity [NTU]", "Fluorescence [Dmnless]"
    if tu in all_df and fl in all_df:
        both_zero = (all_df[tu] == 0) & (all_df[fl] == 0)
        all_df.loc[both_zero, [tu, fl]] = np.nan
        if both_zero.any():
            print(f"  blanked {int(both_zero.sum()):,} samples where turbidity and "
                  f"fluorescence were both exactly zero")

    parts, counts = [], []
    for raw, name in CHANNELS.items():
        if raw not in all_df:
            continue
        d = daily_scalar(all_df, raw, name)
        if d is None:
            continue
        counts.append(d.pop(f"{name}_n"))
        parts.append(d)
    cur, cur_bin = daily_current(all_df)
    if cur is not None:
        print(f"  currents taken from the {cur_bin:g} m bin "
              f"(shallowest with readings on every day)")
        counts.append(cur.pop("cur_n"))
        parts.append(cur)

    if not parts:
        raise SystemExit("no channels survived cleaning")
    day = pd.concat(parts, axis=1, sort=False).sort_index()
    # One number for how much the day rests on, as at Sdot Yam.
    day["samples"] = pd.concat(counts, axis=1, sort=False).max(axis=1).astype("Int64")
    day = day.round(3)
    day.index = day.index.date
    day.index.name = "date"      # after the replacement, or the name is lost

    span = f"{day.index.min()} -> {day.index.max()}"
    print(f"\n{len(day):,} days, {span}")
    for c in day.columns:
        if c.endswith(("_min", "_max")) or c == "samples":
            continue
        n = int(day[c].notna().sum())
        print(f"  {c:<12} {n:>6,} days"
              + (f", {day[c].min():.2f} to {day[c].max():.2f}" if n else ""))

    if args.dry_run:
        print("  dry run: nothing written")
        return 0
    DATA.mkdir(exist_ok=True)
    day.to_csv(OUT)
    print(f"  wrote {OUT.name} ({OUT.stat().st_size / 1024:.0f} kB)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
