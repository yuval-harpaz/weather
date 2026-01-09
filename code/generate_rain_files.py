# import os
# import pandas as pd
from weather import rain_1h
for year in range(1992, 1980, -1):
    y = str(year)
    df_rain = rain_1h(from_date=f'{y}-01-01', to_date=f'{y}-12-31')


df_sta = pd.read_csv('data/ims_stations.csv')
files = sorted(glob('data/rain_*.csv'))
years = [int(f.split('_')[1].split('.')[0]) for f in files]
winters = pd.DataFrame(columns=['winter'])
row = -1
for year in years:
    df = pd.read_csv(f'data/rain_{year}.csv')
    # half2: Jan 1 to Sept 1 of current year
    df2 = df[df['datetime'] < f'{year}-09-01']
    # half2 = np.nansum(df[station][df['datetime'] < f'{year}-09-01'])
    
    if year == years[0]:
        df_prev = df
        continue
    row += 1
    winters.at[row, 'winter'] = str(year-1)+'-'+str(year)
    # half1: Sept 1 of previous year to Jan 1 of current year
    df1 = df_prev[df_prev['datetime'] >= f'{year-1}-09-01']
    # half1 = np.nansum(df_prev[station][df_prev['datetime'] >= f'{year-1}-09-01'])
    df_combined = pd.concat([df1, df2])
    idate = np.where(np.array([md[5:10] for md in df_combined['datetime']]) == '01-03')[0][-1]
    month_day_okay = np.array([False]*len(df_combined))
    month_day_okay[:idate+1] = True
    df_combined = df_combined[month_day_okay]
    for station in df_combined.columns[1:]:
        total = np.nansum(df_combined[station])
        winters.at[row, station] = total
    df_prev = df
    print(year)
winters.to_csv('data/rain_sep_to_aug.csv', index=False)


    
