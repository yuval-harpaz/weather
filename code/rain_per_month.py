import os
import sys
import argparse
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime

# Setup paths
home = os.environ['HOME']
sys.path.append(f'{home}/weather/code')
from weather import round_data

# Load station data for region mapping
df_stations = pd.read_csv('data/ims_stations.csv')

# Regional mapping from user
regions = {
    'יהודה ושומרון': 7,
    'גליל וגולן': 8,
    'עמקי הצפון': 9,
    'ים המלח והערבה': 10,
    'כרמל וחיפה': 11,
    'נגב': 12,
    'גוש דן והשרון': 13,
    'מישור חוף דרומי': 14,
    'מישור חוף צפוני': 15,
}

# Fix regional overrides based on station name
fix = {
    'EDEN FARM 20080706': 'עמקי הצפון',
    'HAIFA PORT': 'מישור חוף צפוני',
    'ROSH HANIQRA_1m': 'מישור חוף צפוני',
    'AFEQ_1m': 'מישור חוף צפוני',
    'GILGAL_1m': 'ים המלח והערבה',
    'GILGAL': 'ים המלח והערבה',
    'ASHDOD PORT_1m': 'מישור חוף דרומי'
}

# Apply fixes to df_stations
for station_name, region_name in fix.items():
    idx = df_stations[df_stations['name'] == station_name].index
    if not idx.empty:
        df_stations.at[idx[0], 'regionId'] = regions[region_name]

# Output file
output_file = 'data/regional_rain_per_month.csv'

# A station is treated as physically reporting in a month if it produced at least this
# many temperature readings that month (~4 days of hourly data).
MIN_TEMP_READINGS = 100

# When at least this fraction of a region's stations recorded rain in a month, the
# region clearly rained, so a station reporting nothing is taken to be a broken gauge
# rather than a dry spot, and is dropped from the regional mean.
WET_FRACTION = 0.75

def base_name(col):
    """'AFEQ_1m' -> 'AFEQ'. The 1-minute and 10-minute gauges are two nearby
    instruments of the same station and are averaged into a single point."""
    return col[:-3] if col.endswith('_1m') else col


# Base station names per region
region_stations = {
    region_name: sorted({base_name(n) for n in df_stations[df_stations['regionId'] == rid]['name']})
    for region_name, rid in regions.items()
}

# Coarse activity window, used only for years with no temperature file (before 1994)
df_activity = pd.read_csv('data/ims_activity.csv')
activity_window = {
    base_name(r['name']): (str(r['earliest'])[:7], str(r['latest'])[:7])
    for _, r in df_activity.iterrows()
}

_temp_cache = {}


def load_temp_year(year):
    """Hourly max-temperature table for a year, or None if there is no file."""
    if year not in _temp_cache:
        path = f'data/temp_max_{year}.csv'
        if os.path.exists(path):
            df = pd.read_csv(path)
            df['datetime'] = pd.to_datetime(df['datetime'])
        else:
            df = None
        _temp_cache[year] = df
    return _temp_cache[year]


def reporting_stations(year, month):
    """Stations known to be alive in a given month.

    The rain files hold rain events only - they never store a zero - so a station
    that is missing from them may be dry or may be dead. The temperature record is
    an independent liveness signal: a station that reported temperatures was there.
    """
    df_temp = load_temp_year(year)
    if df_temp is not None:
        df_month = df_temp[df_temp['datetime'].dt.month == month]
        return {col for col in df_month.columns
                if col != 'datetime' and df_month[col].notna().sum() >= MIN_TEMP_READINGS}
    tag = f'{year}-{month:02d}'
    return {name for name, (first, last) in activity_window.items() if first <= tag <= last}


def working_gauges(df_winter):
    """Stations that recorded rain somewhere in this winter, so the gauge works.

    Picks up rain-only stations that have no temperature sensor, and lets their dry
    months count as real zeros."""
    stations = [col for col in df_winter.columns if col != 'datetime']
    sums = df_winter[stations].sum(skipna=True, min_count=1)
    return {base_name(col) for col in stations if pd.notna(sums[col]) and sums[col] > 0}


def station_amount(sums, station):
    """Monthly rain for one station, averaging its 1-min and 10-min gauges.

    A gauge reading zero beside a wet twin is treated as broken and left out of the
    average, since the two instruments stand next to each other."""
    members = [float(sums[col]) for col in (station, station + '_1m')
               if col in sums.index and pd.notna(sums[col])]
    wet = [v for v in members if v > 0]
    return float(np.mean(wet)) if wet else 0.0


# Month order for winter season (Sept-Aug)
month_order = {m: i for i, m in enumerate([9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8])}
target_months = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]


def get_year_from_winter_and_month(winter_name, month):
    """Get the actual year for a given winter and month."""
    year_start, year_end = map(int, winter_name.split('-'))
    return year_start if month >= 9 else year_end


def get_winter_and_month_for_date(year, month):
    """Get the winter name for a given year and month."""
    if month >= 9:
        return f"{year}-{year+1}", month
    else:
        return f"{year-1}-{year}", month


def load_winter_data(winter_name):
    """Load combined rain data for a winter season."""
    year_start, year_end = map(int, winter_name.split('-'))
    
    prev_file = f'data/rain_{year_start}.csv'
    curr_file = f'data/rain_{year_end}.csv'
    
    if not os.path.exists(prev_file):
        return None
    
    df_prev = pd.read_csv(prev_file)
    df_prev['datetime'] = pd.to_datetime(df_prev['datetime'])
    
    if os.path.exists(curr_file) and year_start != year_end:
        df_curr = pd.read_csv(curr_file)
        df_curr['datetime'] = pd.to_datetime(df_curr['datetime'])
    else:
        df_curr = pd.DataFrame(columns=df_prev.columns)
        df_curr['datetime'] = pd.to_datetime(df_curr['datetime'])
    
    df_sep_dec = df_prev[df_prev['datetime'] >= f'{year_start}-09-01']
    df_jan_aug = df_curr[df_curr['datetime'] < f'{year_end}-09-01']
    
    return pd.concat([df_sep_dec, df_jan_aug])


def compute_month_data(df_winter, m, winter_name, gauges=None):
    """Compute regional rain data for a specific month.

    The regional amount is the mean over the stations that were there to measure it.
    Getting that set right is the whole problem: the rain files store rain events
    only, so a station missing from them is either dry or dead, and counting a dead
    one as a zero drags the mean down while ignoring every zero turns a single wet
    station into the whole region's rainfall.

    A station counts if it reported temperatures that month or if its gauge recorded
    rain somewhere in this winter. On top of that, a station reporting nothing while
    most of its region rained is taken to be broken and dropped.
    """
    df_month = df_winter[df_winter['datetime'].dt.month == m]

    if df_month.empty:
        return []

    results = []
    stations = [col for col in df_month.columns if col != 'datetime']
    monthly_sums = df_month[stations].sum(skipna=True, min_count=1)

    year = get_year_from_winter_and_month(winter_name, m)
    if gauges is None:
        gauges = working_gauges(df_winter)
    present = reporting_stations(year, m) | gauges

    for region_name in regions:
        available_stations = [s for s in region_stations[region_name] if s in present]
        if not available_stations:
            continue

        amounts = np.array([station_amount(monthly_sums, s) for s in available_stations])
        wet = amounts > 0
        if wet.sum() >= WET_FRACTION * len(amounts):
            # The region rained; the stations that measured nothing were not working
            amounts = amounts[wet]

        results.append({
            'Region': region_name,
            'Winter': winter_name,
            'Year': year,
            'Month': m,
            'Rain': round(float(amounts.mean()), 1) if len(amounts) else 0.0
        })
    return results


def update_month_in_df(df_existing, new_data, winter, month):
    """Update or add month data in existing dataframe."""
    if new_data:
        # Remove existing rows for this winter/month
        mask = ~((df_existing['Winter'] == winter) & (df_existing['Month'] == month))
        df_existing = df_existing[mask]
        
        # Add new data
        df_new = pd.DataFrame(new_data)
        df_existing = pd.concat([df_existing, df_new], ignore_index=True)
    
    return df_existing


def process_winter(winter_name):
    """Process all months for a given winter season."""
    df_winter = load_winter_data(winter_name)
    if df_winter is None:
        return []
    
    gauges = working_gauges(df_winter)
    winter_results = []
    for m in target_months:
        month_data = compute_month_data(df_winter, m, winter_name, gauges)
        winter_results.extend(month_data)
    return winter_results


def main():
    parser = argparse.ArgumentParser(description='Update regional rain per month data.')
    parser.add_argument('-f', '--force', action='store_true', help='Force recomputation even if no new data')
    args = parser.parse_args()

    # Current date info
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # Get current and previous month info
    current_winter, curr_m = get_winter_and_month_for_date(current_year, current_month)

    # Previous month
    prev_month_idx = target_months.index(current_month) - 1 if current_month in target_months else -1
    if prev_month_idx < 0:
        # Current is September, previous is August of previous winter
        prev_winter = f"{current_year-1}-{current_year}"
        prev_m = 8
    else:
        prev_m = target_months[prev_month_idx]
        prev_winter = current_winter

    print(f"Current: {current_winter}, month {curr_m}")
    print(f"Previous: {prev_winter}, month {prev_m}")

    # Load existing data
    if os.path.exists(output_file):
        df_existing = pd.read_csv(output_file)
    else:
        df_existing = pd.DataFrame(columns=['Region', 'Winter', 'Year', 'Month', 'Rain'])

    if args.force:
        print("Forcing full recomputation for all years...")
        rain_files = glob('data/rain_*.csv')
        years = sorted([int(f.split('_')[1].split('.')[0]) for f in rain_files])
        winters = [f"{years[i]}-{years[i+1]}" for i in range(len(years)-1)]
        
        all_results = []
        for w in winters:
            print(f"Processing winter {w}...")
            all_results.extend(process_winter(w))
        
        df_existing = pd.DataFrame(all_results)
        updated = True
    else:
        # Load winter data for previous and current months
        df_prev_winter = load_winter_data(prev_winter)
        df_curr_winter = load_winter_data(current_winter)

        updated = False

        # Check and update previous month
        if df_prev_winter is not None:
            new_prev_data = compute_month_data(df_prev_winter, prev_m, prev_winter)
            
            # Compare with existing data
            existing_prev = df_existing[(df_existing['Winter'] == prev_winter) & 
                                         (df_existing['Month'] == prev_m)]
            
            if len(new_prev_data) > 0:
                # Create comparison dataframes
                df_new_prev = pd.DataFrame(new_prev_data).sort_values('Region').reset_index(drop=True)
                df_old_prev = existing_prev.sort_values('Region').reset_index(drop=True)
                
                # Compare Rain values
                if len(df_new_prev) != len(df_old_prev):
                    print(f"Previous month {prev_winter}/{prev_m}: region count changed ({len(df_old_prev)} -> {len(df_new_prev)})")
                    df_existing = update_month_in_df(df_existing, new_prev_data, prev_winter, prev_m)
                    updated = True
                elif not df_new_prev['Rain'].equals(df_old_prev['Rain']):
                    print(f"Previous month {prev_winter}/{prev_m}: rain values updated")
                    df_existing = update_month_in_df(df_existing, new_prev_data, prev_winter, prev_m)
                    updated = True
                else:
                    print(f"Previous month {prev_winter}/{prev_m}: no changes")
        else:
            print(f"Cannot load data for previous winter {prev_winter}")

        # Check and add current month
        if df_curr_winter is not None:
            new_curr_data = compute_month_data(df_curr_winter, curr_m, current_winter)
            
            existing_curr = df_existing[(df_existing['Winter'] == current_winter) & 
                                         (df_existing['Month'] == curr_m)]
            
            if len(new_curr_data) > 0:
                if len(existing_curr) == 0:
                    print(f"Current month {current_winter}/{curr_m}: adding new data ({len(new_curr_data)} regions)")
                    df_existing = update_month_in_df(df_existing, new_curr_data, current_winter, curr_m)
                    updated = True
                else:
                    # Compare and update if different
                    df_new_curr = pd.DataFrame(new_curr_data).sort_values('Region').reset_index(drop=True)
                    df_old_curr = existing_curr.sort_values('Region').reset_index(drop=True)
                    
                    if len(df_new_curr) != len(df_old_curr) or not df_new_curr['Rain'].equals(df_old_curr['Rain']):
                        print(f"Current month {current_winter}/{curr_m}: rain values updated")
                        df_existing = update_month_in_df(df_existing, new_curr_data, current_winter, curr_m)
                        updated = True
                    else:
                        print(f"Current month {current_winter}/{curr_m}: no changes")
            else:
                print(f"Current month {current_winter}/{curr_m}: no data yet")
        else:
            print(f"Cannot load data for current winter {current_winter}")

    # Save if updated
    if updated:
        df_existing['MonthOrder'] = df_existing['Month'].map(month_order)
        df_existing = df_existing.sort_values(['Winter', 'MonthOrder', 'Region']).drop(columns=['MonthOrder'])
        df_existing.to_csv(output_file, index=False)
        print(f"Saved updates to {output_file}")
    else:
        print("No updates needed")


if __name__ == "__main__":
    main()
