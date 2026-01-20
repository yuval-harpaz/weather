import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import *
import numpy as np
from glob import glob
# data = rain_1h(stations=['JERUSALEM GIVAT RAM', 'JERUSALEM CENTRE', 'JERUSALEM GIVAT RAM_1m', 'JERUSALEM CENTRE_1m'], from_date='2025-09-01', to_date='2026-01-19', save_csv='~/Documents/jlm.csv')
# print(np.nansum(data.values[:, 1:], axis=0))
for year in range(2015, 2026):
    # df = pd.read_csv(f'data/rain_{year}.csv')
    # imax = np.nanargmax(df.values[:, 1:], axis=1)
    # print(f'max rain {year}: {df.columns[imax+1]} {np.max(df.values[:, imax+1])}')
    df = pd.read_csv(f'data/temp_min_{year}.csv')
    y = np.nanmin(df.values[:, 1:], axis=0)
    imin = np.nanargmin(y)
    print(f'min temp {year}: {df.columns[imin+1]} {y[imin]}')
    df = pd.read_csv(f'data/temp_max_{year}.csv')
    y = np.nanmax(df.values[:, 1:], axis=0)
    imax = np.nanargmax(y)
    print(f'max temp {year}: {df.columns[imax+1]} {y[imax]}')
    