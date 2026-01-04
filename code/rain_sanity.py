import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import rain_1h, update_stations
import numpy as np

station = 'METZOKE DRAGOT'
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

