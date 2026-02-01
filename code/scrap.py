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

regions = pd.read_csv('data/ims_regions.csv')
j_s = regions['stations'][7]
sta = j_s.split("'name': '")[1:]
sta = [s[:s.index("'")] for s in sta if "Rain" not in s]
