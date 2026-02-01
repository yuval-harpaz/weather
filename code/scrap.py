import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import *
import numpy as np
from glob import glob
from time import sleep
import json

df0 = pd.read_csv('data/rain_2020.csv')
df1 = pd.read_csv('data/rain_2021.csv')
df0 = df0[df0['datetime'] >= '2020-09-01']
df1 = df1[df1['datetime'] < '2021-09-01']
df = pd.concat([df0, df1])
stations = pd.read_csv('data/ims_stations.csv')
stations = np.sort(stations[stations['regionId'] == 7]['name'].tolist())
# winter = pd.DataFrame(columns=[])
for station in stations:
    if station not in df.columns:
        continue
    print(f"{station}: {np.nansum(df[station])}")
    
