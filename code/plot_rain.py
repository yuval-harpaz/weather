import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import datetime
import calendar

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
m_order_map = {m: i for i, m in enumerate(month_order)}

# Get current date for fractional month positioning
now = datetime.datetime.now()
curr_month = now.month
curr_day = now.day
_, days_in_month = calendar.monthrange(now.year, curr_month)
month_fraction = curr_day / days_in_month

# regions = df['Region'].unique()
# regions = regions[[6,4,1,0,8,2,5,7,3]]
regions = ['מישור חוף צפוני','כרמל וחיפה','גליל וגולן','גוש דן והשרון','יהודה ושומרון','עמקי הצפון','מישור חוף דרומי','נגב','ים המלח והערבה']
n_regions = len(regions)
cols = 3
rows = (n_regions + cols - 1) // cols

fig = make_subplots(
    rows=rows, cols=cols,
    subplot_titles=regions,
    vertical_spacing=0.1,
    horizontal_spacing=0.08
)
debug = False
for iregion, region in enumerate(regions):
    if debug:
        iregion = 7
        region = regions[iregion]
    row = (iregion // cols) + 1
    col = (iregion % cols) + 1
    
    # 1. Historical Median
    hist_data = df[(df['Region'] == region) & (df['Winter'].isin(hist_winters))].copy()
    
    # Calculate cumulative sum for historical years
    # To ensure monotonicity and handle missing months, we calculate 
    # the median monthly rainfall first, then take the cumulative sum.
    
    # 1. Prepare data: Ensure every winter has all 12 months (even if 0)
    df_hist_full = pd.DataFrame(columns=month_order)
    for winter in hist_winters:
        for m in month_order:
            val = hist_data[(hist_data['Winter'] == winter) & (hist_data['Month'] == m)]['Rain'].values
            rain = val[0] if len(val) > 0 else 0.0
            df_hist_full.loc[winter, m] = rain
    
    cum_rain = np.cumsum(df_hist_full.values, axis=1)
    med_cum_rain = np.median(cum_rain, axis=0)
    min_cum_rain = cum_rain[np.argmin(cum_rain[:, -1]), :]
    max_cum_rain = cum_rain[np.argmax(cum_rain[:, -1]), :]
    # 1. Historical Range and Median
    x_range = list(range(12))
    fig.add_trace(
        go.Scatter(
            x=x_range,
            y=max_cum_rain,
            mode='lines',
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip'
        ),
        row=row, col=col
    )
    fig.add_trace(
        go.Scatter(
            x=x_range,
            y=min_cum_rain,
            name='טווח 10 שנים',
            mode='lines',
            line=dict(width=0),
            fill='tonexty',
            fillcolor='rgba(200, 200, 200, 0.3)',
            showlegend=(iregion == 0)
        ),
        row=row, col=col
    )
    fig.add_trace(
        go.Scatter(
            x=x_range,
            y=med_cum_rain,
            name='חציון 10 שנים',
            line=dict(color='gray', dash='dash'),
            showlegend=(iregion == 0)
        ),
        row=row, col=col
    )
    
    # 2. Current Winter
    curr_data = df[(df['Region'] == region) & (df['Winter'] == current_winter)].copy()
    if not curr_data.empty:
        curr_data['MonthOrder'] = curr_data['Month'].map(m_order_map)
        curr_data = curr_data.sort_values('MonthOrder')
        curr_data['CumRain'] = np.cumsum(curr_data['Rain'])
        
        x_curr = curr_data['MonthOrder'].tolist()
        # Adjust last point if it's the current month
        last_month_in_data = curr_data['Month'].iloc[-1]
        if last_month_in_data == curr_month:
            # If last point is Oct (index 1), and it's mid-Oct, index should be 0 + fraction
            # Actually, if Jan (index 4) and it's Jan 12, index should be 3 + 12/31
            x_curr[-1] = x_curr[-1] - 1 + month_fraction
            
        fig.add_trace(
            go.Scatter(
                x=x_curr,
                y=curr_data['CumRain'],
                name=current_winter,
                line=dict(color='blue', width=3),
                showlegend=(iregion == 0)
            ),
            row=row, col=col
        )

# Update layout
fig.update_layout(
    title=dict(
        text=f"כמות גשם מצטברת לפי חודש ואיזור: חורף {current_winter} לעומת 10 שנים קודמות",
        x=0.8,
        xanchor='right'
    ),
    height=300 * rows,
    width=1000,
    showlegend=True,
    legend=dict(
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.02
    ),
    template="plotly_white",
    updatemenus=[
        dict(
            type="buttons",
            direction="down",
            buttons=[
                dict(
                    label="Scale 0-900mm",
                    method="relayout",
                    args=[{f"yaxis{i if i > 1 else ''}.range": [0, 900] for i in range(1, n_regions + 1)}]
                ),
                dict(
                    label="Auto Scale",
                    method="relayout",
                    args=[{f"yaxis{i if i > 1 else ''}.autorange": True for i in range(1, n_regions + 1)}]
                ),
            ],
            pad={"r": 10, "t": 10},
            showactive=True,
            x=1.02,
            xanchor="left",
            y=0.7,
            yanchor="top"
        ),
    ]
)

# Set Y-axis title for the first column and update X-axis labels
for r in range(1, rows + 1):
    fig.update_yaxes(title_text="גשם (ממ)", row=r, col=1)
    for c in range(1, cols + 1):
        fig.update_xaxes(
            tickmode='array',
            tickvals=list(range(12)),
            ticktext=[month_names[m] for m in month_order],
            row=r, col=c
        )

# Save the plot
output_path = 'docs/regional_rain.html'
fig.write_html(output_path)

# Post-process to add logo and data source
with open(output_path, 'r', encoding='utf-8') as f:
    white_html = f.read()

# Add favicon
favicon_html = '<link rel="icon" href="logo.jpg">'
white_html = white_html.replace('<head>', f'<head>\n    {favicon_html}')

# Add data source footer
footer_html = """
<div style="position: fixed; bottom: 10px; left: 50%; transform: translateX(-50%); background: rgba(255, 255, 255, 0.8); padding: 5px 10px; border-radius: 5px; font-size: 0.8em; z-index: 1000; direction: rtl; display: flex; align-items: center; gap: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
    <span>נתונים מ</span>
    <a href="https://ims.gov.il/en/data_gov" target="_blank" rel="noopener" style="text-decoration: none; color: inherit; display: flex; align-items: center;">
        <img src="https://ims.gov.il/themes/imst/ims/images/logo.jpg" alt="IMS" style="height: 16px; margin-right: 5px;">
    </a>
</div>
"""
white_html = white_html.replace('</body>', f'{footer_html}\n</body>')

with open(output_path, 'w', encoding='utf-8') as f:
    f.write(white_html)

print(f"Plot saved to {output_path}")
