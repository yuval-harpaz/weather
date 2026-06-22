import os
os.chdir(os.environ['HOME']+'/weather')
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import rain_1h, update_stations, update_activity, round_data
import numpy as np
update_stations()
update_activity(new=True, ignore_old=False)