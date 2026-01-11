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

# Regional mapping from user
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

# Result collection
results = []

# Get all rain files
files = sorted(glob('data/rain_*.csv'))
years = [int(f.split('_')[1].split('.')[0]) for f in files]

# Process across all available consecutive years
for i in range(len(years) - 1):
    year_prev = years[i]
    year_curr = years[i+1]
    
    if year_curr != year_prev + 1:
        continue 

    winter_name = f"{year_prev}-{year_curr}"
    print(f"Processing winter {winter_name}...")
    
    # Load data for both years
    df_prev = pd.read_csv(f'data/rain_{year_prev}.csv')
    df_curr = pd.read_csv(f'data/rain_{year_curr}.csv')
    
    # Ensure datetime is parsed
    df_prev['datetime'] = pd.to_datetime(df_prev['datetime'])
    df_curr['datetime'] = pd.to_datetime(df_curr['datetime'])
    
    # Season: Sept 1 of prev year to Aug 31 of curr year
    df_sep_dec = df_prev[df_prev['datetime'] >= f'{year_prev}-09-01']
    df_jan_aug = df_curr[df_curr['datetime'] < f'{year_curr}-09-01']
    
    # Combine
    df_winter = pd.concat([df_sep_dec, df_jan_aug])
    
    # Months in Sept-Aug cycle
    target_months = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
    for m in target_months:
        df_month = df_winter[df_winter['datetime'].dt.month == m]
        
        if df_month.empty:
            continue
            
        # Sum hourly values per station to get monthly totals
        stations = [col for col in df_month.columns if col != 'datetime']
        monthly_sums = df_month[stations].sum(skipna=True, min_count=1)
        
        # Region-wise aggregation
        for region_name, rid in regions.items():
            region_stations = df_stations[df_stations['regionId'] == rid]['name'].tolist()
            available_stations = [s for s in region_stations if s in monthly_sums.index]
            
            if available_stations:
                region_station_sums = monthly_sums[available_stations].fillna(0)
                if not region_station_sums.empty:
                    median_rain = region_station_sums.median()
                    results.append({
                        'Region': region_name,
                        'Winter': winter_name,
                        'Month': m,
                        'Rain': median_rain
                    })

# Final DataFrame
df_final = pd.DataFrame(results)

if not df_final.empty:
    # Order by Region, Winter, and Month (maintaining Sept-Aug order)
    month_order = {m: i for i, m in enumerate([9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8])}
    df_final['MonthOrder'] = df_final['Month'].map(month_order)
    df_final = df_final.sort_values(['Region', 'Winter', 'MonthOrder']).drop(columns=['MonthOrder'])

    # Round results directly (to avoid issue with round_data failing on mixed types)
    df_final['Rain'] = df_final['Rain'].astype(float).round(1)

    # Save to CSV
    output_file = 'data/regional_rain_per_month.csv'
    df_final.to_csv(output_file, index=False)
    print(f"Done! Created {output_file}")
else:
    print("No monthly data generated.")
