import os
import sys
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import temp_1h, round_data
import pandas as pd
import numpy as np
from glob import glob

# Generate yearly temperature files
years = [2025]
for year in years:
    y = str(year)
    print(f"Collecting TDmin for {year}...")
    df_min = temp_1h(monitor='TDmin', from_date=f'{y}-01-01', to_date=f'{y}-12-31')
    print(f"Collecting TDmax for {year}...")
    df_max = temp_1h(monitor='TDmax', from_date=f'{y}-01-01', to_date=f'{y}-12-31')

# Sum TDmin for winters (Sept-Aug, like rain)
print("\nSummarizing TDmin winters (Sept-Aug)...")
files = sorted(glob('data/temp_min_*.csv'))
years = [int(f.split('_')[-1].split('.')[0]) for f in files]
winters_min = pd.DataFrame(columns=['winter'])
row = -1
for year in years:
    df = pd.read_csv(f'data/temp_min_{year}.csv')
    df2 = df[df['datetime'] < f'{year}-09-01']
    
    if year == years[0]:
        df_prev = df
        continue
    row += 1
    winters_min.at[row, 'winter'] = str(year-1)+'-'+str(year)
    df1 = df_prev[df_prev['datetime'] >= f'{year-1}-09-01']
    df_combined = pd.concat([df1, df2])
    for station in df_combined.columns[1:]:
        # Use mean for temperature instead of sum
        mean_val = np.nanmean(df_combined[station])
        winters_min.at[row, station] = mean_val
    df_prev = df
    print(f"Processed TDmin winter {year-1}-{year}")

if len(winters_min) > 0:
    winters_min.to_csv('data/sum_temp_min_sep_to_aug.csv', index=False)
    round_data('data/sum_temp_min_sep_to_aug.csv')
    print("Saved sum_temp_min_sep_to_aug.csv")

# Sum TDmax for summer cycles (March-Feb)
print("\nSummarizing TDmax summers (March-Feb)...")
files = sorted(glob('data/temp_max_*.csv'))
years = [int(f.split('_')[-1].split('.')[0]) for f in files]
summers_max = pd.DataFrame(columns=['summer'])
row = -1
for year in years:
    df = pd.read_csv(f'data/temp_max_{year}.csv')
    # Part 2: Jan-Feb of current year (belongs to previous cycle)
    df2 = df[df['datetime'] < f'{year}-03-01']
    
    if year == years[0]:
        df_prev = df
        continue
    row += 1
    summers_max.at[row, 'summer'] = str(year-1)+'-'+str(year)
    # Part 1: March-Dec of previous year
    df1 = df_prev[df_prev['datetime'] >= f'{year-1}-03-01']
    df_combined = pd.concat([df1, df2])
    for station in df_combined.columns[1:]:
        # Use mean for temperature
        mean_val = np.nanmean(df_combined[station])
        summers_max.at[row, station] = mean_val
    df_prev = df
    print(f"Processed TDmax summer {year-1}-{year}")

if len(summers_max) > 0:
    summers_max.to_csv('data/sum_temp_max_mar_to_feb.csv', index=False)
    round_data('data/sum_temp_max_mar_to_feb.csv')
    print("Saved sum_temp_max_mar_to_feb.csv")

print("\nDone!")
