import os
import pandas as pd
import sys
from datetime import datetime
import numpy as np

# Setup paths
home = os.environ['HOME']
sys.path.append(f'{home}/weather/code')
from weather import temp_1h, update_stations, update_activity, round_data

# # Update station metadata
# update_stations()
# update_activity()

# Get current date info
now = datetime.now()
y = str(now.year)
m = str(now.month).zfill(2)
d = str(now.day).zfill(2)
to_date = f'{y}-{m}-{d}'

df_sta = pd.read_csv('data/ims_stations.csv')
df_act = pd.read_csv('data/ims_activity.csv')

def update_monitor(monitor_type):
    """
    Update TDmin or TDmax data incrementally.
    monitor_type: 'TDmin' or 'TDmax'
    """
    prefix = 'temp_min' if monitor_type == 'TDmin' else 'temp_max'
    opcsv = f'data/{prefix}_{y}.csv'
    
    if not os.path.exists(opcsv):
        print(f"Creating new file for {monitor_type} {y}...")
        df_temp = temp_1h(monitor=monitor_type, from_date=f'{y}-01-01', to_date=to_date, save_csv=opcsv)
    else:
        print(f"Updating {monitor_type} {y} incrementally...")
        df_temp = pd.read_csv(opcsv)
        
        # Identify stations that should have this monitor and where active when laast checked
        latest_act = df_act['latest'].max()[:10]
        active_stations = df_act[df_act['latest'] >= latest_act]['name'].tolist()
        
        for i, sta in enumerate(active_stations):
            # Check if station supports this monitor
            monitors_str = df_sta[df_sta['name'] == sta]['monitors'].values
            if len(monitors_str) == 0 or f"'{monitor_type}'" not in monitors_str[0]:
                continue
            
            # Find last non-NaN date for this station
            if sta in df_temp.columns:
                last_idx = np.where(~df_temp[sta].isna())[0]
                if len(last_idx) == 0:
                    last_date = f'{y}-01-01'
                else:
                    last_date = df_temp['datetime'].iloc[last_idx[-1]][:10]
            else:
                last_date = f'{y}-01-01'
                df_temp[sta] = np.nan
            
            if last_date >= to_date:
                continue
            
            print(f"\rUpdating {monitor_type} for {sta} ({i+1}/{len(active_stations)})...", end="", flush=True)
            
            # Query new data (save_csv=False to handle merging manually)
            df_new = temp_1h(monitor=monitor_type, stations=[sta], from_date=last_date, to_date=to_date, save_csv=False)
            
            if df_new.empty or sta not in df_new.columns:
                continue
                
            # Merge new data into df_temp
            # Ensure df_temp has all datetimes from df_new
            new_datetimes = df_new['datetime'].values
            missing_datetimes = [dt for dt in new_datetimes if dt not in df_temp['datetime'].values]
            
            if missing_datetimes:
                df_missing = pd.DataFrame({'datetime': missing_datetimes})
                for col in df_temp.columns:
                    if col != 'datetime':
                        df_missing[col] = np.nan
                df_temp = pd.concat([df_temp, df_missing]).sort_values('datetime').reset_index(drop=True)
            
            # Fill in the temperature values
            for _, row in df_new.iterrows():
                dt = row['datetime']
                val = row[sta]
                if pd.notna(val):
                    df_temp.loc[df_temp['datetime'] == dt, sta] = val
        
        print(f"\nSaving updated {monitor_type} data to {opcsv}")
        df_temp.to_csv(opcsv, index=False)
        round_data(opcsv)

if __name__ == "__main__":
    update_monitor('TDmin')
    update_monitor('TDmax')
    print("Temperature updates complete.")
