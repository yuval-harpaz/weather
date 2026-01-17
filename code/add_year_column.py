"""
One-time script to add Year column to regional_rain_per_month.csv
and re-sort by Winter, Month (9-12 then 1-8 order), then Region.
"""
import pandas as pd

# Read existing CSV
csv_path = 'data/regional_rain_per_month.csv'
df = pd.read_csv(csv_path)

print(f"Original shape: {df.shape}")
print(f"Original columns: {list(df.columns)}")

# Add Year column based on Winter and Month
# For months 9-12: Year = first year from Winter
# For months 1-8: Year = second year from Winter
def get_year(row):
    winter = row['Winter']  # e.g., "2024-2025"
    month = row['Month']
    year_start, year_end = winter.split('-')
    if month >= 9:
        return int(year_start)
    else:
        return int(year_end)

df['Year'] = df.apply(get_year, axis=1)

# Sort by Winter, then Month (9-12, 1-8 order), then Region
month_order = {m: i for i, m in enumerate([9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8])}
df['MonthOrder'] = df['Month'].map(month_order)
df = df.sort_values(['Winter', 'MonthOrder', 'Region']).drop(columns=['MonthOrder'])

# Reorder columns: Region, Winter, Year, Month, Rain
df = df[['Region', 'Winter', 'Year', 'Month', 'Rain']]

# Save back to CSV
df.to_csv(csv_path, index=False)

print(f"Updated shape: {df.shape}")
print(f"Updated columns: {list(df.columns)}")
print(f"\nFirst 20 rows:")
print(df.head(20).to_string())
print(f"\nLast 20 rows:")
print(df.tail(20).to_string())
