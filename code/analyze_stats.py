import pandas as pd
from scipy import stats
import os

def main():
    data_path = '/home/yuval/weather/data/daily_comparison.csv'
    
    if not os.path.exists(data_path):
        print(f"Error: {data_path} not found. Run compare_forecast.py first.")
        return

    df = pd.read_csv(data_path)
    
    # Exclude Elat and En Gedi as requested in previous steps for absolute error analysis
    exclude_locs = ['Elat', 'En Gedi']
    df_filtered = df[~df['Location'].isin(exclude_locs)].copy()
    
    base_lead = 1
    compare_leads = [2, 3, 4]
    
    for temp_type in ['Max', 'Min']:
        print(f"\nRunning dependent samples t-tests on {temp_type} Temp Absolute Errors (excluding {', '.join(exclude_locs)})")
        print("-" * 80)
        
        actual_col_base = f'Actual_{temp_type}'
        
        # Calculate absolute errors for all relevant leads
        for lead in [1, 2, 3, 4]:
            pred_col = f"{temp_type.lower()} -{lead}"
            df_filtered[f'abs_err_{temp_type}_{lead}'] = (df_filtered[pred_col] - df_filtered[actual_col_base]).abs()
        
        for lead in compare_leads:
            col_base = f'abs_err_{temp_type}_{base_lead}'
            col_compare = f'abs_err_{temp_type}_{lead}'
            
            paired_data = df_filtered[[col_base, col_compare]].dropna()
            
            if len(paired_data) < 2:
                print(f"{temp_type} Lead -{lead} vs -{base_lead}: Not enough paired data (n={len(paired_data)})")
                continue
                
            t_stat, p_val = stats.ttest_rel(paired_data[col_compare], paired_data[col_base])
            
            print(f"Comparison: {temp_type} Lead -{lead} vs Lead -{base_lead}")
            print(f"  Sample size (n): {len(paired_data)}")
            print(f"  Mean Abs Error (-{lead}): {paired_data[col_compare].mean():.4f}")
            print(f"  Mean Abs Error (-{base_lead}): {paired_data[col_base].mean():.4f}")
            print(f"  t-statistic: {t_stat:.4f}")
            print(f"  p-value:     {p_val:.4g}")
            
            if p_val < 0.05:
                print("  Result: Significant difference (p < 0.05)")
            else:
                print("  Result: No significant difference")
            print("-" * 80)

if __name__ == "__main__":
    main()
