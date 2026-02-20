import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def main():
    data_path = '/home/yuval/weather/data/daily_comparison.csv'
    output_path = '/home/yuval/weather/data/forecast_errors.html'
    
    if not os.path.exists(data_path):
        print(f"Error: {data_path} not found. Run compare_forecast.py first.")
        return

    df = pd.read_csv(data_path)
    
    # Create subplots: 3 rows, 2 cols
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            "Max Temp: Error vs Measured", "Min Temp: Error vs Measured",
            "Max Temp: Absolute Error (Exc. Desert)", "Min Temp: Absolute Error (Exc. Desert)",
            "Max Temp: Predicted vs Actual", "Min Temp: Predicted vs Actual"
        ),
        vertical_spacing=0.08,
        horizontal_spacing=0.1
    )

    leads = [1, 2, 3, 4]
    colors = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA'] # Plotly default colors
    desert_locs = ['Elat', 'En Gedi']

    # ROW 1: Scatter plots (Error vs Measured)
    for i, lead in enumerate(leads):
        for col, temp_type in enumerate(['Max', 'Min'], start=1):
            pred_col = f'{temp_type.lower()} -{lead}'
            actual_col = f'Actual_{temp_type}'
            valid_df = df[[actual_col, pred_col, 'Location', 'Date']].dropna()
            
            if not valid_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=valid_df[actual_col],
                        y=valid_df[pred_col] - valid_df[actual_col],
                        mode='markers',
                        name=f'Lead -{lead}',
                        marker=dict(size=3, color=colors[i]),
                        legendgroup=f'lead{lead}',
                        showlegend=(col == 1),
                        customdata=valid_df[['Location', 'Date']],
                        hovertemplate="<b>%{customdata[0]}</b><br>Date: %{customdata[1]}<br>Measured: %{x}°C<br>Error: %{y}°C<extra></extra>"
                    ),
                    row=1, col=col
                )

    # ROW 2: Box plots (Absolute Error distribution)
    for i, lead in enumerate(leads):
        for col, temp_type in enumerate(['Max', 'Min'], start=1):
            pred_col = f'{temp_type.lower()} -{lead}'
            actual_col = f'Actual_{temp_type}'
            valid_df = df[[actual_col, pred_col, 'Location']].dropna()
            # Exclude desert locs as requested for box plots
            valid_df = valid_df[~valid_df['Location'].isin(desert_locs)]
            
            if not valid_df.empty:
                abs_err = (valid_df[pred_col] - valid_df[actual_col]).abs()
                fig.add_trace(
                    go.Box(
                        y=abs_err,
                        name=f'Lead -{lead}',
                        marker_color=colors[i],
                        legendgroup=f'lead{lead}',
                        showlegend=False
                    ),
                    row=2, col=col
                )

    # ROW 3: Predicted vs Actual Scatter
    # Traces for Standard locations (by lead)
    for i, lead in enumerate(leads):
        for col, temp_type in enumerate(['Max', 'Min'], start=1):
            pred_col = f'{temp_type.lower()} -{lead}'
            actual_col = f'Actual_{temp_type}'
            valid_df = df[[actual_col, pred_col, 'Location', 'Date']].dropna()
            valid_std = valid_df[~valid_df['Location'].isin(desert_locs)]
            
            if not valid_std.empty:
                fig.add_trace(
                    go.Scatter(
                        x=valid_std[actual_col],
                        y=valid_std[pred_col],
                        mode='markers',
                        name=f'Lead -{lead}',
                        marker=dict(size=3, color=colors[i]),
                        legendgroup=f'lead{lead}',
                        showlegend=False,
                        customdata=valid_std[['Location', 'Date']],
                        hovertemplate="<b>%{customdata[0]}</b><br>Date: %{customdata[1]}<br>Actual: %{x}°C<br>Pred: %{y}°C<extra></extra>"
                    ),
                    row=3, col=col
                )

    # Trace for Desert locations (Black)
    for col, temp_type in enumerate(['Max', 'Min'], start=1):
        actual_col = f'Actual_{temp_type}'
        # Combine all leads for desert locations into one black group
        desert_data_list = []
        for lead in leads:
            pred_col = f'{temp_type.lower()} -{lead}'
            temp_df = df[[actual_col, pred_col, 'Location', 'Date']].dropna()
            desert_data_list.append(temp_df[temp_df['Location'].isin(desert_locs)])
        
        valid_desert = pd.concat(desert_data_list)
        
        if not valid_desert.empty:
            # Drop actuals/preds to identify correct columns dynamically
            # Wait, the concat above might have different pred_cols. 
            # Let's fix the logic to get x and y correctly from combined leads.
            
            # Corrected logic for combined desert trace
            all_desert_x = []
            all_desert_y = []
            all_desert_meta = []
            for lead in leads:
                p_col = f'{temp_type.lower()} -{lead}'
                a_col = f'Actual_{temp_type}'
                t_df = df[[a_col, p_col, 'Location', 'Date']].dropna()
                t_df = t_df[t_df['Location'].isin(desert_locs)]
                all_desert_x.extend(t_df[a_col].tolist())
                all_desert_y.extend(t_df[p_col].tolist())
                all_desert_meta.extend(t_df[['Location', 'Date']].values.tolist())
            
            fig.add_trace(
                go.Scatter(
                    x=all_desert_x,
                    y=all_desert_y,
                    mode='markers',
                    name='Desert (Elat/En Gedi)',
                    marker=dict(size=3, color='black'),
                    legendgroup='desert',
                    showlegend=(col == 1),
                    customdata=all_desert_meta,
                    hovertemplate="<b>%{customdata[0]}</b><br>Date: %{customdata[1]}<br>Actual: %{x}°C<br>Pred: %{y}°C<extra></extra>"
                ),
                row=3, col=col
            )
            
            # Add one-to-one line (ideal prediction)
            min_val = min(min(all_desert_x), min(all_desert_y)) if all_desert_x else 0
            max_val = max(max(all_desert_x), max(all_desert_y)) if all_desert_x else 40
            # Get range from all data in subplot
            fig.add_shape(
                type="line", x0=0, y0=0, x1=45, y1=45,
                line=dict(color="Gray", width=1, dash="dash"),
                row=3, col=col
            )

    # Update axes labels
    fig.update_xaxes(title_text="Measured Temp (°C)", row=1, col=1)
    fig.update_xaxes(title_text="Measured Temp (°C)", row=1, col=2)
    fig.update_yaxes(title_text="Error (Pred - Actual) (°C)", row=1, col=1)
    fig.update_yaxes(title_text="Error (Pred - Actual) (°C)", row=1, col=2)
    
    fig.update_yaxes(title_text="Abs. Error (°C)", row=2, col=1)
    fig.update_yaxes(title_text="Abs. Error (°C)", row=2, col=2)
    
    fig.update_xaxes(title_text="Measured Temp (°C)", row=3, col=1)
    fig.update_xaxes(title_text="Measured Temp (°C)", row=3, col=2)
    fig.update_yaxes(title_text="Predicted Temp (°C)", row=3, col=1)
    fig.update_yaxes(title_text="Predicted Temp (°C)", row=3, col=2)

    fig.update_layout(
        title_text="Comprehensive Weather Forecast Analysis (2026)",
        template="plotly_white",
        height=1400,
        width=1200,
        legend_title_text="Lead / Group",
        boxmode='group'
    )

    # Save to HTML
    fig.write_html(output_path)
    print(f"Final 3x2 visualization saved to {output_path}")

if __name__ == "__main__":
    main()
