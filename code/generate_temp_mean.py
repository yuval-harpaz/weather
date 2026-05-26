"""
Generate data/regional_temp_mean_per_month.csv

For each year, reads temp_min_YYYY.csv and temp_max_YYYY.csv (hourly data).
For each station and day:
    daily_midrange = (daily_max + daily_min) / 2
Then for each (year, month):
    - mean of daily midranges across days in that month -> per-station monthly value
    - median across stations belonging to the same region -> regional value

The "Cycle" in the output is simply the calendar year (e.g. "2024"),
so that the HTML chart can display Jan-Dec on the x-axis.
"""
import os
import sys
import pandas as pd
import numpy as np
from glob import glob

# Setup paths so we can run from any directory
home = os.environ.get('HOME', os.path.expanduser('~'))
base_dir = os.path.join(home, 'weather')

# Load station data for region mapping
df_stations = pd.read_csv(os.path.join(base_dir, 'data', 'ims_stations.csv'))

# Regional mapping (matching temp_per_month.py)
regions = {
    'יהודה ושומרון': 7,
    'גליל וגולן': 8,
    'עמקי הצפון': 9,
    'ים המלח והערבה': 10,
    'כרמל וחיפה': 11,
    'נגב': 12,
    'גוש דן והשרון': 13,
    'מישור חוף דרומי': 14,
    'מישור חוף צפוני': 15,
}

# Fix regional overrides based on station name (matching temp_per_month.py)
fix = {
    'EDEN FARM 20080706': 'עמקי הצפון',
    'HAIFA PORT': 'מישור חוף צפוני',
    'ROSH HANIQRA_1m': 'מישור חוף צפוני',
    'AFEQ_1m': 'מישור חוף צפוני',
    'GILGAL_1m': 'ים המלח והערבה',
    'ASHDOD PORT_1m': 'מישור חוף דרומי'
}

for station_name, region_name in fix.items():
    idx = df_stations[df_stations['name'] == station_name].index
    if not idx.empty:
        df_stations.at[idx[0], 'regionId'] = regions[region_name]


def process_year(year):
    """
    Process one year: compute monthly mean midrange per station,
    then median across region, returning a list of dicts.
    """
    min_file = os.path.join(base_dir, 'data', f'temp_min_{year}.csv')
    max_file = os.path.join(base_dir, 'data', f'temp_max_{year}.csv')

    if not os.path.exists(min_file) or not os.path.exists(max_file):
        print(f"  Skipping {year}: missing min or max file")
        return []

    df_min = pd.read_csv(min_file)
    df_max = pd.read_csv(max_file)

    df_min['datetime'] = pd.to_datetime(df_min['datetime'])
    df_max['datetime'] = pd.to_datetime(df_max['datetime'])

    # Station columns (exclude datetime)
    min_stations = [c for c in df_min.columns if c != 'datetime']
    max_stations = [c for c in df_max.columns if c != 'datetime']
    common_stations = list(set(min_stations) & set(max_stations))

    if not common_stations:
        print(f"  Skipping {year}: no common stations")
        return []

    # Resample to daily: min of TDmin, max of TDmax
    df_min = df_min.set_index('datetime')
    df_max = df_max.set_index('datetime')

    daily_min = df_min[common_stations].resample('D').min()
    daily_max = df_max[common_stations].resample('D').max()

    # Daily midrange per station
    daily_mid = (daily_min + daily_max) / 2.0

    # Add month and year columns
    daily_mid['_month'] = daily_mid.index.month
    daily_mid['_year'] = daily_mid.index.year

    results = []

    for (yr, month), group in daily_mid.groupby(['_year', '_month']):
        cycle = str(yr)

        # Monthly mean of daily midranges per station
        station_values = group[common_stations].mean()

        if station_values.isna().all():
            continue

        for region_name, rid in regions.items():
            region_stations = df_stations[df_stations['regionId'] == rid]['name'].tolist()
            available = [s for s in region_stations if s in common_stations]

            if available:
                region_vals = station_values[available].dropna()
                if not region_vals.empty:
                    median_val = region_vals.median()
                    results.append({
                        'Region': region_name,
                        'Cycle': cycle,
                        'Month': month,
                        'Temp': round(float(median_val), 1)
                    })

    return results


def main():
    output_file = os.path.join(base_dir, 'data', 'regional_temp_mean_per_month.csv')

    min_files = sorted(glob(os.path.join(base_dir, 'data', 'temp_min_*.csv')))
    years = [int(os.path.basename(f).replace('temp_min_', '').replace('.csv', ''))
             for f in min_files]

    all_results = []
    for year in years:
        print(f"Processing {year}...")
        all_results.extend(process_year(year))

    if not all_results:
        print("No results generated.")
        return

    df_final = pd.DataFrame(all_results)

    # Sort by Cycle (year), Month, Region
    df_final['Cycle'] = df_final['Cycle'].astype(str)
    df_final = df_final.sort_values(['Cycle', 'Month', 'Region'])

    df_final.to_csv(output_file, index=False)
    print(f"Saved {output_file} ({len(df_final)} rows)")


if __name__ == '__main__':
    main()

