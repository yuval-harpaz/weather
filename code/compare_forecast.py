import pandas as pd
import os

# Approved station mapping
STATION_MAPPING = {
    'Afula': 'AFULA NIR HAEMEQ',
    'Ashdod': 'ASHDOD PORT',
    'Beer Sheva': 'BEER SHEVA BGU',
    'Bet Shean': 'EDEN FARM',
    'Elat': 'ELAT',
    'En Gedi': 'METZOKE DRAGOT',
    'Haifa': 'HAIFA UNIVERSITY',
    'Jerusalem': 'JERUSALEM CENTRE',
    'Lod': 'BET DAGAN',
    'Mizpe Ramon': 'MIZPE RAMON',
    'Nazareth': 'NAZARETH',
    'Qazrin': 'GAMLA',
    'Tel Aviv - Yafo': 'TEL AVIV COAST',
    'Tiberias': 'TIBERIAS',
    'Zefat': 'ZEFAT HAR KENAAN',
}

def load_and_aggregate_actuals(file_path, agg_func):
    """Loads hourly data and aggregates to daily values."""
    df = pd.read_csv(file_path)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    
    # Drop datetime and group by date
    daily = df.drop(columns=['datetime']).groupby('date').agg(agg_func).reset_index()
    return daily

def main():
    data_dir = '/home/yuval/weather/data'
    
    print("Loading measurements...")
    actual_max_daily = load_and_aggregate_actuals(os.path.join(data_dir, 'temp_max_2026.csv'), 'max')
    actual_min_daily = load_and_aggregate_actuals(os.path.join(data_dir, 'temp_min_2026.csv'), 'min')
    
    print("Loading and processing predictions...")
    pred_df = pd.read_csv(os.path.join(data_dir, 'predictions.csv'))
    
    # Parse dates
    pred_df['IssueDateTime'] = pd.to_datetime(pred_df['IssueDateTime'])
    pred_df['IssueDate'] = pred_df['IssueDateTime'].dt.date
    pred_df['Date'] = pd.to_datetime(pred_df['Date']).dt.date
    
    # Calculate Lead Time: (Date - IssueDate) + 1
    # Lead 1: Issued on the same day (e.g. at 4am)
    # Lead 2: Issued 1 day before
    # ...
    pred_df['Lead'] = (pred_df['Date'] - pred_df['IssueDate']).apply(lambda x: x.days + 1)
    
    # Filter for relevant leads
    pred_df = pred_df[(pred_df['Lead'] >= 1) & (pred_df['Lead'] <= 4)]
    
    # For each (Date, Location, Lead), pick the earliest issue time (prioritizing 4am release)
    # We sort by Date, Location, Lead, and IssueDateTime ASC
    pred_df = pred_df.sort_values(['Date', 'LocationNameEng', 'Lead', 'IssueDateTime'])
    
    # Group by Date, Location, Lead and take the first (earliest) one that has at least one temperature
    # but the user specifically wants the 4am one for -1.
    # Actually, taking the first valid one usually picks the 4am release if it exists.
    
    # Let's perform a pivot-like aggregation
    locations = pred_df['LocationNameEng'].unique()
    dates = sorted(pred_df['Date'].unique())
    
    results = []
    
    for date in dates:
        for loc_eng in locations:
            row = {
                'Date': date,
                'Location': loc_eng,
            }
            
            # Map to station and get actuals
            station_name = STATION_MAPPING.get(loc_eng)
            if station_name:
                row['Actual_Max'] = None
                if station_name in actual_max_daily.columns:
                    val = actual_max_daily[actual_max_daily['date'] == date][station_name].values
                    if len(val) > 0: row['Actual_Max'] = val[0]
                
                row['Actual_Min'] = None
                if station_name in actual_min_daily.columns:
                    val = actual_min_daily[actual_min_daily['date'] == date][station_name].values
                    if len(val) > 0: row['Actual_Min'] = val[0]
            
            # Fill leads
            for lead in range(1, 5):
                # Filter for this specific lead
                lead_data = pred_df[(pred_df['Date'] == date) & 
                                    (pred_df['LocationNameEng'] == loc_eng) & 
                                    (pred_df['Lead'] == lead)]
                
                if not lead_data.empty:
                    # Pick the best one:
                    # If lead is 1, prioritize the 4am release (first in sorted list)
                    # But we should skip the afternoon release of the same day if possible 
                    # for the 'max' because it's usually empty.
                    
                    best_max = None
                    best_min = None
                    
                    # Iterate to find the first non-NaN if possible, 
                    # but prioritize the earliest one of the day for max/min.
                    for _, p_row in lead_data.iterrows():
                        if pd.notna(p_row['Maximum temperature']) and best_max is None:
                            best_max = p_row['Maximum temperature']
                        if pd.notna(p_row['Minimum temperature']) and best_min is None:
                            best_min = p_row['Minimum temperature']
                        if best_max is not None and best_min is not None:
                            break
                    
                    row[f'max -{lead}'] = best_max
                    row[f'min -{lead}'] = best_min
                else:
                    row[f'max -{lead}'] = None
                    row[f'min -{lead}'] = None
            
            results.append(row)
            
    # Convert to DataFrame
    comparison_df = pd.DataFrame(results)
    
    # Define column order
    cols = ['Date', 'Location', 'Actual_Max', 'Actual_Min', 
            'max -1', 'min -1', 'max -2', 'min -2', 
            'max -3', 'min -3', 'max -4', 'min -4']
    
    # Ensure all columns exist even if all NaN
    for c in cols:
        if c not in comparison_df.columns:
            comparison_df[c] = None
            
    comparison_df = comparison_df[cols]
    
    # Save to CSV
    output_path = os.path.join(data_dir, 'daily_comparison.csv')
    comparison_df.to_csv(output_path, index=False)
    print(f"Comparison table saved to {output_path}")
    
    # Check the specific case the user mentioned: Afula on 2026-02-06
    # 2026-02-06 04:25	2026-02-06	עפולה	Afula	11	19
    # This should be Lead 1 (Feb 6 - Feb 6 + 1 = 1)
    # Output this specific row if it exists
    sample = comparison_df[(comparison_df['Date'] == pd.Timestamp('2026-02-06').date()) & 
                           (comparison_df['Location'] == 'Afula')]
    if not sample.empty:
        print("\nVerification for Afula on 2026-02-06:")
        print(sample.to_string(index=False))
    else:
        print("\nWarning: Could not find verification row for Afula on 2026-02-06")

if __name__ == "__main__":
    main()
