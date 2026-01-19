import os
import sys
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime

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

def process_temp_data(monitor_type):
    """
    Process temperature data for a specific monitor type.
    monitor_type: 'min' or 'max'
    """
    prefix = f'temp_{monitor_type}'
    files = sorted(glob(f'data/{prefix}_*.csv'))
    
    all_data = []
    
    for file in files:
        year = int(file.split('_')[-1].split('.')[0])
        print(f"Processing {prefix} for {year}...")
        df = pd.read_csv(file)
        if df.empty:
            print(f"File {file} is empty.")
            continue
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['Month'] = df['datetime'].dt.month
        
        # Determine the cycle (Winter for min, Summer for max)
        if monitor_type == 'min':
            df['Cycle'] = df.apply(lambda row: f"{row['datetime'].year-1}-{row['datetime'].year}" if row['datetime'].month < 9 else f"{row['datetime'].year}-{row['datetime'].year+1}", axis=1)
        else:
            df['Cycle'] = df.apply(lambda row: f"{row['datetime'].year-1}-{row['datetime'].year}" if row['datetime'].month < 3 else f"{row['datetime'].year}-{row['datetime'].year+1}", axis=1)

        stations = [col for col in df.columns if col not in ['datetime', 'Month', 'Cycle']]
        
        # For each cycle and month, compute regional medians
        for (cycle, month), group in df.groupby(['Cycle', 'Month']):
            print(f"  Cycle: {cycle}, Month: {month}")
            # For each station, find the min/max in this month
            if monitor_type == 'min':
                station_values = group[stations].min()
            else:
                station_values = group[stations].max()
            
            if station_values.isna().all():
                print(f"    WARNING: All station values are NaN for {cycle} {month}")
                continue

            for region_name, rid in regions.items():
                region_stations = df_stations[df_stations['regionId'] == rid]['name'].tolist()
                available_stations = [s for s in region_stations if s in stations]
                
                if available_stations:
                    # Get the values for stations in this region
                    region_vals = station_values[available_stations].dropna()
                    if not region_vals.empty:
                        median_val = region_vals.median()
                        all_data.append({
                            'Region': region_name,
                            'Cycle': cycle,
                            'Month': month,
                            'Temp': round(float(median_val), 1)
                        })
    print(f"Finished processing all files for {monitor_type}.")
    
    output_df = pd.DataFrame(all_data)
    output_file = f'data/regional_temp_{monitor_type}_per_month.csv'
    
    # Sort data
    if monitor_type == 'min':
        month_order = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
    else:
        month_order = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2]
    
    m_order_map = {m: i for i, m in enumerate(month_order)}
    output_df['MonthOrder'] = output_df['Month'].map(m_order_map)
    output_df = output_df.sort_values(['Cycle', 'MonthOrder', 'Region']).drop(columns=['MonthOrder'])
    
    output_df.to_csv(output_file, index=False)
    round_data(output_file)
    print(f"Saved {output_file}")

if __name__ == "__main__":
    process_temp_data('min')
    process_temp_data('max')
    print("Done!")
