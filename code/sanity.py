import os
import pandas as pd
import sys
from datetime import datetime
sys.path.append(os.environ['HOME']+'/weather/code')
from weather import rain_1h, update_stations
import numpy as np
import unittest
from glob import glob
rain_years = np.sort([int(f.split('/')[-1][5:-4]) for f in glob('*data/rain_*.csv')])
class TestRain(unittest.TestCase):
    def dates_unique(self):
        passed = np.zeros(len(rain_years), bool)
        for iyear, year in enumerate(rain_years):
            rain = pd.read_csv(f'data/rain_{year}.csv')
            passed[iyear] = len(rain['datetime'].unique()) == len(rain)
        if not np.all(passed):
            print(f'Rain files do not have unique dates: {rain_years[~passed]}')
        self.assertTrue(np.all(passed))

    def dates_sequential(self):
        passed = np.zeros(len(rain_years), bool)
        for iyear, year in enumerate(rain_years):
            rain = pd.read_csv(f'data/rain_{year}.csv')
            diff = pd.to_datetime(rain['datetime']).diff()
            passed[iyear] = len(np.unique(diff.values[1:])) == 1
        if not np.all(passed):
            print(f'Rain files do not have uniqueunique time deltas: {rain_years[~passed]}')

    def dates_full_year(self):
        passed = np.zeros(len(rain_years), bool)
        current_year = datetime.now().year
        for iyear, year in enumerate(rain_years):
            rain = pd.read_csv(f'data/rain_{year}.csv')
            start_ok = rain['datetime'][0] == str(year)+'-01-01 00:00'
            end_ok = (year == current_year) or (rain['datetime'].iloc[-1] == str(year)+'-12-31 23:00')
            passed[iyear] = start_ok and end_ok
        if not np.all(passed):
            print(f'Rain files do not cover full year: {rain_years[~passed]}')
        self.assertTrue(np.all(passed))


df_stations = pd.read_csv('data/ims_stations.csv')
df_active = pd.read_csv('data/ims_activity.csv')
class TestStations(unittest.TestCase):
    def stations_unique(self):
        passed = np.zeros(len(rain_years), bool)
        for iyear, year in enumerate(rain_years):
            rain = pd.read_csv(f'data/rain_{year}.csv')
            passed[iyear] = len(rain.columns[1:].unique()) == len(rain.columns[1:])
        if not np.all(passed):
            print(f'Rain files do not have unique stations: {rain_years[~passed]}')
        self.assertTrue(np.all(passed))
        n_stations = len(df_stations)
        self.assertEqual(df_stations.shape[0], len(df_stations['name'].unique()))
        self.assertEqual(df_active.shape[0], len(df_active['name'].unique()))

    def n_stations(self):
        n_sta =  df_stations.shape[0]
        n_act =  df_active.shape[0]


if __name__ == '__main__':
    args = sys.argv
    # weather_results = unittest.TestResult()
    if len(args) == 1:
        weather_suite = unittest.TestSuite(tests=[TestRain  ('dates_unique'),
                                              TestRain('dates_sequential'),
                                              TestRain('dates_full_year'),
                                              TestStations('stations_unique'),
                                              TestStations('n_stations'),
                                              ]
                                       )
        unittest.TextTestRunner(verbosity=2).run(weather_suite)


