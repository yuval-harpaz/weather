"""
monitor_update.py

Incrementally collect hourly-averaged data for a set of IMS monitors
(Grad, RH, WS, WD) and store them as yearly CSV files under data/.

Usage:
    python code/monitor_update.py            # uses YEAR defined in __main__
    python code/monitor_update.py 2024       # override year from command line
"""

import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime

home = os.environ['HOME']
sys.path.append(f'{home}/weather/code')
from weather import monitor_1h, round_data

# Monitors to collect
MONITORS = ['Grad', 'RH', 'WS', 'WD']

df_sta = pd.read_csv('data/ims_stations.csv')
df_act = pd.read_csv('data/ims_activity.csv')

now = datetime.now()
today = now.strftime('%Y-%m-%d')


def update_monitor(monitor, year):
    """
    Update a single monitor's yearly CSV incrementally.

    For a new file  → queries the full year up to today.
    For an existing file → queries only the gap between the last recorded
    reading and today for each active station.
    """
    prefix = monitor.lower()
    opcsv = f'data/{prefix}_{year}.csv'
    to_date = today

    if not os.path.exists(opcsv):
        print(f"Creating new file for {monitor} {year}...")
        monitor_1h(monitor=monitor, from_date=f'{year}-01-01', to_date=to_date, save_csv=opcsv)
    else:
        print(f"Updating {monitor} {year} incrementally...")
        df_mon = pd.read_csv(opcsv)

        # Active stations: those last seen at the most recent activity date
        latest_act = df_act['latest'].max()[:10]
        active_stations = df_act[df_act['latest'] >= latest_act]['name'].tolist()

        for i, sta in enumerate(active_stations):
            if '_1m' in sta:
                continue

            # Check station supports this monitor
            monitors_str = df_sta[df_sta['name'] == sta]['monitors'].values
            if len(monitors_str) == 0 or f"'{monitor}'" not in monitors_str[0]:
                continue

            # Find last non-NaN date for this station
            if sta in df_mon.columns:
                last_idx = np.where(~df_mon[sta].isna())[0]
                if len(last_idx) == 0:
                    last_date = f'{year}-01-01'
                else:
                    last_date = df_mon['datetime'].iloc[last_idx[-1]][:10]
            else:
                last_date = f'{year}-01-01'
                df_mon[sta] = np.nan

            if last_date >= to_date:
                continue

            print(f"\rUpdating {monitor} for {sta} ({i+1}/{len(active_stations)})...", end='', flush=True)

            df_new = monitor_1h(monitor=monitor, stations=[sta],
                                from_date=last_date, to_date=to_date, save_csv=False)

            if df_new.empty or sta not in df_new.columns:
                continue

            # Extend df_mon with any new datetime rows
            new_datetimes = df_new['datetime'].values
            missing = [dt for dt in new_datetimes if dt not in df_mon['datetime'].values]
            if missing:
                df_missing = pd.DataFrame({'datetime': missing})
                for col in df_mon.columns:
                    if col != 'datetime':
                        df_missing[col] = np.nan
                df_mon = pd.concat([df_mon, df_missing]).sort_values('datetime').reset_index(drop=True)

            # Fill in new values
            for _, row in df_new.iterrows():
                dt = row['datetime']
                val = row[sta]
                if pd.notna(val):
                    df_mon.loc[df_mon['datetime'] == dt, sta] = val

        print(f"\nSaving updated {monitor} data to {opcsv}")
        df_mon.to_csv(opcsv, index=False)
        round_data(opcsv)


if __name__ == '__main__':
    # Default year – change here for manual backfills, or pass as CLI argument
    year = str(now.year)  # current year: 2026

    if len(sys.argv) == 2:
        year = sys.argv[1]
    elif len(sys.argv) > 2:
        print("Usage: python monitor_update.py [YEAR]")
        sys.exit(1)

    for mon in MONITORS:
        update_monitor(mon, year)

    print("Monitor updates complete.")

