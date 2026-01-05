import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import rain_1h, update_stations
import numpy as np

station = 'METZOKE DRAGOT'
if False:
    winters = []
    for year in range(2015, 2027):
        df = pd.read_csv(f'data/rain_{year}.csv')
        # half2: Jan 1 to Sept 1 of current year
        half2 = np.nansum(df[station][df['datetime'] < f'{year}-09-01'])
        
        if year == 2015:
            df_prev = df
            continue
        
        # half1: Sept 1 of previous year to Jan 1 of current year
        half1 = np.nansum(df_prev[station][df_prev['datetime'] >= f'{year-1}-09-01'])
        
        total = half1 + half2
        winters.append(total)
        df_prev = df

winters = []
for year in range(2015, 2027):
    df = pd.read_csv(f'data/rain_{year}.csv')
    # half2: Jan 1 to Sept 1 of current year
    df2 = df[df['datetime'] < f'{year}-09-01']
    # half2 = np.nansum(df[station][df['datetime'] < f'{year}-09-01'])
    
    if year == 2015:
        df_prev = df
        continue
    
    # half1: Sept 1 of previous year to Jan 1 of current year
    df1 = df_prev[df_prev['datetime'] >= f'{year-1}-09-01']
    # half1 = np.nansum(df_prev[station][df_prev['datetime'] >= f'{year-1}-09-01'])
    df_combined = pd.concat([df1, df2])
    idate = np.where(np.array([md[5:10] for md in df_combined['datetime']]) == '01-03')[0][-1]
    month_day_okay = np.array([False]*len(df_combined))
    month_day_okay[:idate+1] = True
    df_combined = df_combined[month_day_okay]
    total = np.nansum(df_combined[station])
    winters.append(total)
    df_prev = df

print(winters[-1]/np.median(winters[:-1]))