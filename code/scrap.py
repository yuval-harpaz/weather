import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import *
import numpy as np
from glob import glob
data = rain_1h(stations=['JERUSALEM CENTRE_1m'], from_date='2026-01-12', to_date='2026-01-14', save_csv=True)
df_active = pd.read_csv('data/ims_activity.csv')
jers = df_active['name'][df_active['name'].str.contains('Jerusalem'.upper())].values
start = []
for ista in range(len(jers)):
    start.append(int(df_active['earliest'][df_active['name'] == jers[ista]].values[0][:4]))
for ista, station in enumerate(jers):
    times = []
    rain = []
    since = None
    for year in range(start[ista], 2027):
        df = pd.read_csv(f'data/rain_{year}.csv')
        if station in df.columns:
            if since is None:
                since = year
            times.extend(pd.to_datetime(df['datetime'].values))
            rain.extend(df[station].values)
    # if '_1m' in station:
    #     window = 24*60
    # else:
    #     window = 24*6
    rain_smoothed = smooth(np.array(rain), 24, method='sum')
    imax = np.argmax(rain_smoothed)
    vmax = rain_smoothed[imax]
    tmax = times[imax]
    print(f'{station} since {since}: {vmax:.2f} mm at {tmax}')
    print('last 48h:', np.max(rain_smoothed[-48:]))
print('done')
    
