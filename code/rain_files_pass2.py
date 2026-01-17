import os
import sys
sys.path.append(os.environ['HOME']+'/weather/code')
# import pandas as pd
from weather import rain_1h, round_data
import pandas as pd
import numpy as np
from glob import glob
# get all the data
years = list(range(1989,2026))
for year in years:
    y = str(year)
    df_rain = rain_1h(from_date=f'{y}-01-01', to_date=f'{y}-12-31', save_csv=False)
    df_rain.to_csv(f'~/Documents/rain_{y}.csv', index=False)
    df = pd.read_csv(f'data/rain_{y}.csv')
    missing_in_db = []
    for col in df_rain.columns[1:]:
        if col not in df.columns:
            missing_in_db.append(col)
    if len(missing_in_db) > 0:
        print(f'missing in db: {missing_in_db}')
        err = os.system(f"echo '{y} missing in db: {','.join(missing_in_db)} ' >> ~/Documents/rain_issues.txt")
    missing_in_test = []
    for col in df.columns[1:]:
        if col not in df_rain.columns:
            missing_in_test.append(col)
    if len(missing_in_test) > 0:
        print(f'missing in test: {missing_in_test}')
        err = os.system(f"echo '{y} missing in test: {','.join(missing_in_test)} ' >> ~/Documents/rain_issues.txt")
    for col in df_rain.columns[1:]:
        if col in df.columns:
            rain = np.nansum(df[col])
            rain_test = np.nansum(df_rain[col])
            if np.round(rain) != np.round(rain_test):
                print(f'rain for {col} is {rain:.1f} in db and {rain_test:.1f} in test')
                err = os.system(f"echo '{y} rain for {col} is {rain:.1f} in db and {rain_test:.1f} in test' >> ~/Documents/rain_issues.txt")

print('Done!')
# print(df_rain)

# print(df)
# print('debug')
# # sum winters
# df_sta = pd.read_csv('data/ims_stations.csv')
# files = sorted(glob('data/rain_*.csv'))
# years = [int(f.split('_')[1].split('.')[0]) for f in files]
# winters = pd.DataFrame(columns=['winter'])
# row = -1
# for year in years:
#     df = pd.read_csv(f'data/rain_{year}.csv')
#     # half2: Jan 1 to Sept 1 of current year
#     df2 = df[df['datetime'] < f'{year}-09-01']
    
#     if year == years[0]:
#         df_prev = df
#         continue
#     row += 1
#     winters.at[row, 'winter'] = str(year-1)+'-'+str(year)
#     # half1: Sept 1 of previous year to Jan 1 of current year
#     df1 = df_prev[df_prev['datetime'] >= f'{year-1}-09-01']
#     df_combined = pd.concat([df1, df2])
#     for station in df_combined.columns[1:]:
#         total = np.nansum(df_combined[station])
#         winters.at[row, station] = total
#     df_prev = df
#     print(year)
# winters.to_csv('data/sum_rain_sep_to_aug.csv', index=False)
# round_data('data/sum_rain_sep_to_aug.csv')



