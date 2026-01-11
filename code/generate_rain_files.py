import os
import sys
sys.path.append(os.environ['HOME']+'/weather/code')
# import pandas as pd
from weather import rain_1h, round_data
import pandas as pd
import numpy as np
from glob import glob
# get all the data
years = [1947]
for year in years:
    y = str(year)
    df_rain = rain_1h(from_date=f'{y}-01-01', to_date=f'{y}-12-31')

# sum winters
df_sta = pd.read_csv('data/ims_stations.csv')
files = sorted(glob('data/rain_*.csv'))
years = [int(f.split('_')[1].split('.')[0]) for f in files]
winters = pd.DataFrame(columns=['winter'])
row = -1
for year in years:
    df = pd.read_csv(f'data/rain_{year}.csv')
    # half2: Jan 1 to Sept 1 of current year
    df2 = df[df['datetime'] < f'{year}-09-01']
    
    if year == years[0]:
        df_prev = df
        continue
    row += 1
    winters.at[row, 'winter'] = str(year-1)+'-'+str(year)
    # half1: Sept 1 of previous year to Jan 1 of current year
    df1 = df_prev[df_prev['datetime'] >= f'{year-1}-09-01']
    df_combined = pd.concat([df1, df2])
    for station in df_combined.columns[1:]:
        total = np.nansum(df_combined[station])
        winters.at[row, station] = total
    df_prev = df
    print(year)
winters.to_csv('data/sum_rain_sep_to_aug.csv', index=False)
round_data('data/sum_rain_sep_to_aug.csv')



