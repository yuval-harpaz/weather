import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Load the data
df = pd.read_csv('data/regional_rain_per_month.csv')

# Define winter periods
# Last 10 winters before current
winters = sorted(df['Winter'].unique())
current_winter = winters[-1]
hist_winters = winters[-11:-1]

# Define month order (Sept to Aug)
month_order = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
month_names = {
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec',
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
    5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug'
}

regions = df['Region'].unique()
n_regions = len(regions)
cols = 3
rows = (n_regions + cols - 1) // cols

fig = make_subplots(
    rows=rows, cols=cols,
    subplot_titles=regions,
    vertical_spacing=0.1,
    horizontal_spacing=0.08
)

for i, region in enumerate(regions):
    row = (i // cols) + 1
    col = (i % cols) + 1
    
    # 1. Historical Median
    hist_data = df[(df['Region'] == region) & (df['Winter'].isin(hist_winters))].copy()
    
    # Calculate cumulative sum for historical years
    # To ensure monotonicity and handle missing months, we calculate 
    # the median monthly rainfall first, then take the cumulative sum.
    
    # 1. Prepare data: Ensure every winter has all 12 months (even if 0)
    hist_list = []
    for winter in hist_winters:
        for m in month_order:
            val = hist_data[(hist_data['Winter'] == winter) & (hist_data['Month'] == m)]['Rain'].values
            rain = val[0] if len(val) > 0 else 0.0
            hist_list.append({'Winter': winter, 'Month': m, 'Rain': rain})
    
    df_hist_full = pd.DataFrame(hist_list)
    m_order_map = {m: idx for idx, m in enumerate(month_order)}
    df_hist_full['MonthOrder'] = df_hist_full['Month'].map(m_order_map)
    
    # 2. Calculate median MONTHLY rain across years
    median_monthly = df_hist_full.groupby('MonthOrder')['Rain'].median().reset_index()
    # 3. Cumulative sum of the medians
    median_monthly['CumRain'] = median_monthly['Rain'].cumsum()
    
    fig.add_trace(
        go.Scatter(
            x=[month_names[m] for m in month_order],
            y=median_monthly['CumRain'],
            name='10-Year Median',
            line=dict(color='gray', dash='dash'),
            showlegend=(i == 0)
        ),
        row=row, col=col
    )
    
    # 2. Current Winter
    curr_data = df[(df['Region'] == region) & (df['Winter'] == current_winter)].copy()
    if not curr_data.empty:
        curr_data['MonthOrder'] = curr_data['Month'].map(m_order_map)
        curr_data = curr_data.sort_values('MonthOrder')
        curr_data['CumRain'] = curr_data['Rain'].cumsum()
        
        fig.add_trace(
            go.Scatter(
                x=[month_names[m] for m in curr_data['Month']],
                y=curr_data['CumRain'],
                name=current_winter,
                line=dict(color='blue', width=3),
                showlegend=(i == 0)
            ),
            row=row, col=col
        )

# Update layout
fig.update_layout(
    title_text=f"Cumulative Monthly Rainfall by Region: {current_winter} vs 10-Year Median",
    height=300 * rows,
    width=1000,
    showlegend=True,
    template="plotly_white"
)

# Set Y-axis title for the first column
for r in range(1, rows + 1):
    fig.update_yaxes(title_text="Rain (mm)", row=r, col=1)

# Save the plot
output_path = 'docs/regional_rain.html'
fig.write_html(output_path)
print(f"Plot saved to {output_path}")
