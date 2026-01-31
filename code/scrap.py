import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import *
import numpy as np
from glob import glob
from time import sleep
next_week = (datetime.now()+timedelta(days=7)).strftime('%Y-%m-%d')
yesterday = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
station = 'EZUZ'
data = query_rain(station=station, from_date='2026-01-29', to_date=next_week)
current = data[-1]['datetime']
for test in range(25*6):  # run a test every 10 minutes for 25 hours
    
    print('sampling data at '+datetime.now().strftime('%H:%M:%S'))
    data = query_temp(station=station, from_date='2026-01-29', to_date=next_week, monitor='TDmin')
    if data[-1]['datetime'] > current:
        current = data[-1]['datetime']
        print(f'new data found at {current}')
    sleep(60*10)
print(f"last check finished at {datetime.now().strftime('%H:%M:%S')}")
