import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import datetime
import calendar
import os

def generate_temp_plot(monitor_type):
    """
    Generates a regional temperature plot for a specific monitor type.
    monitor_type: 'min' or 'max'
    """
    input_file = f'data/regional_temp_{monitor_type}_per_month.csv'
    if not os.path.exists(input_file):
        print(f"Input file {input_file} not found. Skipping.")
        return

    df = pd.read_csv(input_file)
    
    # Define cycles and current
    cycles = sorted(df['Cycle'].unique())
    current_cycle = cycles[-1]
    hist_cycles = cycles[-11:-1]
    
    # Define month order
    if monitor_type == 'min':
        month_order = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8]
        title_prefix = "טמפרטורת מינימום"
        y_label = "טמפרטורה (C°)"
        cycle_name = "חורף"
    else:
        month_order = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2]
        title_prefix = "טמפרטורת מקסימום"
        y_label = "טמפרטורה (C°)"
        cycle_name = "מחזור"

    month_names = {
        9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec',
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
        5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug'
    }
    m_order_map = {m: i for i, m in enumerate(month_order)}
    
    # Get current date info
    now = datetime.datetime.now()
    curr_month = now.month
    curr_day = now.day
    _, days_in_month = calendar.monthrange(now.year, curr_month)
    month_fraction = curr_day / days_in_month

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

    for iregion, region in enumerate(regions):
        row = (iregion // cols) + 1
        col = (iregion % cols) + 1
        
        # 1. Historical Data
        hist_data = df[(df['Region'] == region) & (df['Cycle'].isin(hist_cycles))].copy()
        
        if hist_data.empty:
            continue

        # Prepare 10-year range data
        hist_matrix = np.full((len(hist_cycles), 12), np.nan)
        for i, cycle in enumerate(hist_cycles):
            for m in month_order:
                val = hist_data[(hist_data['Cycle'] == cycle) & (df['Month'] == m)]['Temp'].values
                if len(val) > 0:
                    hist_matrix[i, m_order_map[m]] = val[0]
        
        med_temp = np.nanmedian(hist_matrix, axis=0)
        min_temp = np.nanmin(hist_matrix, axis=0)
        max_temp = np.nanmax(hist_matrix, axis=0)
        
        x_range = list(range(12))
        
        # Historical Range
        fig.add_trace(
            go.Scatter(
                x=x_range, y=max_temp, mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'
            ), row=row, col=col
        )
        fig.add_trace(
            go.Scatter(
                x=x_range, y=min_temp, name='טווח 10 שנים', mode='lines', line=dict(width=0),
                fill='tonexty', fillcolor='rgba(200, 200, 200, 0.3)', showlegend=(iregion == 0)
            ), row=row, col=col
        )
        # Historical Median
        fig.add_trace(
            go.Scatter(
                x=x_range, y=med_temp, name='חציון 10 שנים', line=dict(color='gray', dash='dash'), showlegend=(iregion == 0)
            ), row=row, col=col
        )

        # 2. Current Cycle
        curr_data = df[(df['Region'] == region) & (df['Cycle'] == current_cycle)].copy()
        if not curr_data.empty:
            curr_data['MonthOrder'] = curr_data['Month'].map(m_order_map)
            # Filter out months that are in the future or don't have data
            curr_data = curr_data.dropna(subset=['MonthOrder']).sort_values('MonthOrder')
            
            x_curr = curr_data['MonthOrder'].tolist()
            y_curr = curr_data['Temp'].tolist()
            
            # Adjust last point if it's the current month
            last_month_in_data = curr_data['Month'].iloc[-1]
            if last_month_in_data == curr_month:
                x_curr[-1] = x_curr[-1] - 1 + month_fraction
                
            fig.add_trace(
                go.Scatter(
                    x=x_curr, y=y_curr, name=current_cycle, line=dict(color='blue', width=3), showlegend=(iregion == 0)
                ), row=row, col=col
            )

    # Layout updates
    if monitor_type == 'min':
        scale_label = "Scale -5 to 30°C"
        scale_range = [-5, 30]
    else:
        scale_label = "Scale 15 to 50°C"
        scale_range = [15, 50]

    fig.update_layout(
        title=dict(
            text=f"{title_prefix} לפי חודש ואיזור: {cycle_name} {current_cycle} לעומת 10 שנים קודמות",
            x=0.8, xanchor='right'
        ),
        height=300 * rows, width=1000, showlegend=True,
        legend=dict(yanchor="top", y=1, xanchor="left", x=1.02),
        template="plotly_white",
        updatemenus=[
            dict(
                type="buttons", direction="down",
                buttons=[
                    dict(
                        label=scale_label,
                        method="relayout",
                        args=[{f"yaxis{i if i > 1 else ''}.range": scale_range for i in range(1, n_regions + 1)}]
                    ),
                    dict(
                        label="Auto Scale",
                        method="relayout",
                        args=[{f"yaxis{i if i > 1 else ''}.autorange": True for i in range(1, n_regions + 1)}]
                    ),
                ],
                pad={"r": 10, "t": 10}, showactive=True, x=1.02, xanchor="left", y=0.7, yanchor="top"
            ),
        ]
    )

    for r in range(1, rows + 1):
        fig.update_yaxes(title_text=y_label, row=r, col=1)
        for c in range(1, cols + 1):
            fig.update_xaxes(
                tickmode='array', tickvals=list(range(12)),
                ticktext=[month_names[m] for m in month_order], row=r, col=c
            )

    output_path = f'docs/regional_temp_{monitor_type}.html'
    fig.write_html(output_path)

    # Post-process for favicon and footer (copied from plot_rain.py)
    with open(output_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    favicon_html = '<link rel="icon" href="logo.jpg">'
    html_content = html_content.replace('<head>', f'<head>\n    {favicon_html}')
    footer_html = """
<div style="position: fixed; bottom: 10px; left: 50%; transform: translateX(-50%); background: rgba(255, 255, 255, 0.8); padding: 5px 10px; border-radius: 5px; font-size: 0.8em; z-index: 1000; direction: rtl; display: flex; align-items: center; gap: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
    <span>נתונים מ</span>
    <a href="https://ims.gov.il/en/data_gov" target="_blank" rel="noopener" style="text-decoration: none; color: inherit; display: flex; align-items: center;">
        <img src="https://ims.gov.il/themes/imst/ims/images/logo.jpg" alt="IMS" style="height: 16px; margin-right: 5px;">
    </a>
</div>
"""
    html_content = html_content.replace('</body>', f'{footer_html}\n</body>')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"Plot saved to {output_path}")

if __name__ == "__main__":
    generate_temp_plot('min')
    generate_temp_plot('max')
