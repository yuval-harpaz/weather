import os
import sys
import pandas as pd
import numpy as np
import argparse
from glob import glob
from datetime import datetime, timedelta

# Setup paths
home = os.environ['HOME']
sys.path.append(f'{home}/weather/code')
from weather import round_data

# Load station data for region mapping
df_stations = pd.read_csv('data/ims_stations.csv')

# Regional mapping (matching rain_per_month.py)
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

# Fix regional overrides based on station name
fix = {
    'EDEN FARM 20080706': 'עמקי הצפון',
    'HAIFA PORT': 'מישור חוף צפוני',
    'ROSH HANIQRA_1m': 'מישור חוף צפוני',
    'AFEQ_1m': 'מישור חוף צפוני',
    'GILGAL_1m': 'ים המלח והערבה',
    'ASHDOD PORT_1m': 'מישור חוף דרומי'
}

# Apply fixes to df_stations
for station_name, region_name in fix.items():
    idx = df_stations[df_stations['name'] == station_name].index
    if not idx.empty:
        df_stations.at[idx[0], 'regionId'] = regions[region_name]

def get_cycle_and_month(date, monitor_type):
    """Determine the cycle name and month for a given date and monitor type."""
    m = date.month
    y = date.year
    if monitor_type == 'min':
        # Sept-Aug cycle
        if m >= 9:
            return f"{y}-{y+1}", m
        else:
            return f"{y-1}-{y}", m
    else:
        # Mar-Feb cycle
        if m >= 3:
            return f"{y}-{y+1}", m
        else:
            return f"{y-1}-{y}", m

def compute_monthly_medians(df, monitor_type):
    """Compute regional medians from a dataframe of hourly data."""
    if df.empty:
        return []
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['Month'] = df['datetime'].dt.month
    
    if monitor_type == 'min':
        df['Cycle'] = df.apply(lambda row: f"{row['datetime'].year-1}-{row['datetime'].year}" if row['datetime'].month < 9 else f"{row['datetime'].year}-{row['datetime'].year+1}", axis=1)
    else:
        df['Cycle'] = df.apply(lambda row: f"{row['datetime'].year-1}-{row['datetime'].year}" if row['datetime'].month < 3 else f"{row['datetime'].year}-{row['datetime'].year+1}", axis=1)

    stations = [col for col in df.columns if col not in ['datetime', 'Month', 'Cycle']]
    results = []
    
    for (cycle, month), group in df.groupby(['Cycle', 'Month']):
        if monitor_type == 'min':
            station_values = group[stations].min()
        else:
            station_values = group[stations].max()
        
        if station_values.isna().all():
            continue

        for region_name, rid in regions.items():
            region_stations = df_stations[df_stations['regionId'] == rid]['name'].tolist()
            available_stations = [s for s in region_stations if s in stations]
            
            if available_stations:
                region_vals = station_values[available_stations].dropna()
                if not region_vals.empty:
                    median_val = region_vals.median()
                    results.append({
                        'Region': region_name,
                        'Cycle': cycle,
                        'Month': month,
                        'Temp': round(float(median_val), 1)
                    })
    return results

def process_temp_data(monitor_type, force=False):
    output_file = f'data/regional_temp_{monitor_type}_per_month.csv'
    prefix = f'temp_{monitor_type}'
    
    if monitor_type == 'min':
        month_order = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
    else:
        month_order = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2]
    
    m_order_map = {m: i for i, m in enumerate(month_order)}

    if force or not os.path.exists(output_file):
        print(f"Full recompute for {prefix}...")
        files = sorted(glob(f'data/{prefix}_*.csv'))
        all_results = []
        for file in files:
            df = pd.read_csv(file)
            all_results.extend(compute_monthly_medians(df, monitor_type))
        df_final = pd.DataFrame(all_results)
    else:
        print(f"Incremental update for {prefix}...")
        df_final = pd.read_csv(output_file)
        
        # Determine last two months
        now = datetime.now()
        prev_month_date = now.replace(day=1) - timedelta(days=1)
        
        targets = [prev_month_date, now]
        for target_date in targets:
            cycle, month = get_cycle_and_month(target_date, monitor_type)
            print(f" Checking {cycle} Month {month}...")
            
            # Load corresponding year files
            # A cycle Year1-Year2 might span two files: temp_type_Year1.csv and temp_type_Year2.csv
            # But usually a month belongs to one file.
            year = target_date.year
            file = f'data/{prefix}_{year}.csv'
            if os.path.exists(file):
                df_year = pd.read_csv(file)
                df_year['datetime'] = pd.to_datetime(df_year['datetime'])
                df_month = df_year[df_year['datetime'].dt.month == month]
                
                new_data = compute_monthly_medians(df_month, monitor_type)
                if new_data:
                    # Remove old entries for this cycle/month
                    df_final = df_final[~((df_final['Cycle'] == cycle) & (df_final['Month'] == month))]
                    df_final = pd.concat([df_final, pd.DataFrame(new_data)], ignore_index=True)

    # Sort and save
    df_final['MonthOrder'] = df_final['Month'].map(m_order_map)
    df_final = df_final.sort_values(['Cycle', 'MonthOrder', 'Region']).drop(columns=['MonthOrder'])
    df_final.to_csv(output_file, index=False)
    print(f"Saved {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update regional temperature per month data.')
    parser.add_argument('-f', '--force', action='store_true', help='Force full recomputation')
    args = parser.parse_args()

    process_temp_data('min', force=args.force)
    process_temp_data('max', force=args.force)
    print("Done!")
