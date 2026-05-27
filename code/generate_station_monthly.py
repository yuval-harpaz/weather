import os
import pandas as pd
from glob import glob
import numpy as np
'''
Create data/station_monthly.csv from data/rain_*.csv, data/temp_min_*.csv, data/temp_max_*.csv
'''
def get_cycle(date, measure_type):
    """
    Determine cycle (winter year) based on date and measure type.
    Rain & Min Temp: Sept (9) starts new cycle.
    Max Temp: March (3) starts new cycle.
    """
    month = date.month
    year = date.year
    
    if measure_type in ['Rain', 'MinTemp']:
        if month >= 9:
            return f"{year}-{year+1}"
        else:
            return f"{year-1}-{year}"
    elif measure_type == 'MaxTemp':
        if month >= 3:
            return f"{year}-{year+1}"
        else:
            return f"{year-1}-{year}"
    return f"{year}"

def process_files(file_pattern, measure_name, agg_func):
    """
    Process files matching pattern, aggregate by month/cycle, and return list of records.
    """
    files = sorted(glob(file_pattern))
    results = []
    
    print(f"Processing {measure_name}...")
    for file in files:
        print(f"  Reading {file}...")
        try:
            df = pd.read_csv(file)
            if df.empty:
                continue
            
            # Ensure datetime
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Assign Cycle
            # Vectorized cycle assignment is faster
            if measure_name in ['Rain', 'MinTemp']:
                df['Cycle'] = np.where(df['datetime'].dt.month >= 9,
                                     df['datetime'].dt.year.astype(str) + '-' + (df['datetime'].dt.year + 1).astype(str),
                                     (df['datetime'].dt.year - 1).astype(str) + '-' + df['datetime'].dt.year.astype(str))
            else: # MaxTemp
                df['Cycle'] = np.where(df['datetime'].dt.month >= 3,
                                     df['datetime'].dt.year.astype(str) + '-' + (df['datetime'].dt.year + 1).astype(str),
                                     (df['datetime'].dt.year - 1).astype(str) + '-' + df['datetime'].dt.year.astype(str))
            
            df['Month'] = df['datetime'].dt.month
            df['Year'] = df['datetime'].dt.year
            
            # Identify station columns (exclude non-station cols)
            station_cols = [c for c in df.columns if c not in ['datetime', 'Cycle', 'Month', 'Year']]
            
            # Group by Cycle, Month
            # We also include 'Year' in groupby effectively by taking metadata, but Year varies within a Cycle. 
            # We want Month-Year unique identification strictly speaking, but Cycle-Month is what we group by for the dashboard? 
            # No, the dashboard needs specific Year data (e.g. 2024) to plot specific lines.
            # So we group by Cycle, Month. Note that (Cycle, Month) -> Unique Year-Month.
            # E.g. Cycle 2023-2024, Month 9 -> Sep 2023. Cycle 2023-2024, Month 1 -> Jan 2024.
            
            grouped = df.groupby(['Cycle', 'Month'])
            
            for (cycle, month), group in grouped:
                # Calculate aggregate value for all stations
                if agg_func == 'sum':
                    vals = group[station_cols].sum(min_count=1) # min_count=1 ensures all-NaN returns NaN, not 0
                elif agg_func == 'min':
                    vals = group[station_cols].min()
                elif agg_func == 'max':
                    vals = group[station_cols].max()
                
                # Create records
                # Filter out NaNs to save space
                vals = vals.dropna()
                
                for station, value in vals.items():
                    results.append({
                        'Station': station,
                        'Measure': measure_name,
                        'Cycle': cycle,
                        'Month': month,
                        'Value': round(float(value), 1)
                    })
                    
        except Exception as e:
            print(f"Error processing {file}: {e}")
            
    return results

def process_mean_files():
    """
    For each year that has BOTH temp_min and temp_max files, compute the
    daily midrange (max+min)/2 per station, then mean across days in each
    calendar month.  Cycle = calendar year string (e.g. '2024').
    """
    min_files = sorted(glob('data/temp_min_*.csv'))
    years = [int(os.path.basename(f).replace('temp_min_', '').replace('.csv', ''))
             for f in min_files]
    results = []
    print("Processing MeanTemp...")
    for year in years:
        min_file = f'data/temp_min_{year}.csv'
        max_file = f'data/temp_max_{year}.csv'
        if not os.path.exists(max_file):
            continue
        try:
            df_min = pd.read_csv(min_file)
            df_max = pd.read_csv(max_file)
            df_min['datetime'] = pd.to_datetime(df_min['datetime'])
            df_max['datetime'] = pd.to_datetime(df_max['datetime'])

            common_stations = [c for c in df_min.columns
                               if c != 'datetime' and c in df_max.columns]
            if not common_stations:
                continue

            df_min = df_min.set_index('datetime')
            df_max = df_max.set_index('datetime')

            # Daily min of TDmin, daily max of TDmax, then midrange
            daily_min = df_min[common_stations].resample('D').min()
            daily_max = df_max[common_stations].resample('D').max()
            daily_mid = (daily_min + daily_max) / 2.0

            daily_mid['_month'] = daily_mid.index.month
            daily_mid['_year']  = daily_mid.index.year

            for (yr, month), group in daily_mid.groupby(['_year', '_month']):
                vals = group[common_stations].mean().dropna()
                cycle = str(yr)
                for station, value in vals.items():
                    results.append({
                        'Station': station,
                        'Measure': 'MeanTemp',
                        'Cycle': cycle,
                        'Month': month,
                        'Value': round(float(value), 1)
                    })
            print(f"  Processed MeanTemp {year}")
        except Exception as e:
            print(f"Error processing MeanTemp {year}: {e}")
    return results


def main():
    all_data = []
    
    # 1. Rain (Sum)
    all_data.extend(process_files('data/rain_*.csv', 'Rain', 'sum'))
    
    # 2. Min Temp (Min)
    all_data.extend(process_files('data/temp_min_*.csv', 'MinTemp', 'min'))
    
    # 3. Max Temp (Max)
    all_data.extend(process_files('data/temp_max_*.csv', 'MaxTemp', 'max'))

    # 4. Mean Temp (midrange mean)
    all_data.extend(process_mean_files())
    
    # Save to CSV
    output_file = 'data/station_monthly.csv'
    print(f"Saving {len(all_data)} records to {output_file}...")
    
    df_out = pd.DataFrame(all_data)
    # Sort for better compression/readability
    df_out = df_out.sort_values(['Station', 'Measure', 'Cycle', 'Month'])
    
    df_out.to_csv(output_file, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
