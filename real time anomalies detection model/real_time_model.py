import os
import threading
import pandas as pd
import numpy as np
import logging
import time
from collections import deque

import dash
from dash.dependencies import Output, Input
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from sklearn.ensemble import IsolationForest

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

# Thread-safe data storage
data_lock = threading.Lock()

# Data structures for each power plant type
plant_types = ['Gas Plant', 'Wind Farm', 'Solar Farm', 'Hydroelectric Plant']

# Define features for each plant type
plant_features = {
    'Gas Plant': ['power_output', 'demand', 'fuel_consumption', 'emissions'],
    'Wind Farm': ['power_output', 'demand', 'wind_speed', 'turbine_efficiency'],
    'Solar Farm': ['power_output', 'demand', 'solar_radiation', 'panel_temperature'],
    'Hydroelectric Plant': ['power_output', 'demand', 'water_flow_rate', 'turbine_rotation_speed']
}

# Initialize data storage with a sliding window
window_size = 500  # Adjust based on memory and performance
data_store = {
    plant_type: {
        'data': deque(maxlen=window_size),
        'outliers': pd.DataFrame()
    } for plant_type in plant_types
}

def start_spark_streaming():
    """
    Start Spark Structured Streaming to read data from Kafka and process it in real-time.
    """
    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName("EnergyStreamOutlierDetection") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "app") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Define schema for incoming data
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("plant_type", StringType()),
        StructField("region", StringType()),
        StructField("power_output", DoubleType()),
        StructField("demand", DoubleType()),
        StructField("grid_frequency", DoubleType()),
        StructField("fuel_consumption", DoubleType()),
        StructField("emissions", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("turbine_efficiency", DoubleType()),
        StructField("solar_radiation", DoubleType()),
        StructField("panel_temperature", DoubleType()),
        StructField("water_flow_rate", DoubleType()),
        StructField("turbine_rotation_speed", DoubleType())
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')) \
        .option("subscribe", "energy_stream") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    # Parse the JSON data
    df = df.selectExpr("CAST(value AS STRING)")
    df_parsed = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Convert timestamp to proper format
    df_parsed = df_parsed.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Function to process each batch
    def process_batch(batch_df, batch_id):
        """
        Process each micro-batch of data from Spark Streaming.

        Args:
            batch_df (DataFrame): The batch data as a Spark DataFrame.
            batch_id (int): The batch ID.
        """
        try:
            pandas_df = batch_df.toPandas()
            logging.info(f"Processing batch {batch_id} with {len(pandas_df)} records")

            with data_lock:
                for plant_type in plant_types:
                    # Filter data for the current plant type
                    plant_df = pandas_df[pandas_df['plant_type'] == plant_type]
                    if plant_df.empty:
                        continue

                    # Select relevant features
                    features = ['timestamp'] + plant_features[plant_type]
                    plant_df = plant_df[features].dropna()

                    if plant_df.empty:
                        continue

                    # Data Validation: Check data types and ranges
                    for feature in plant_features[plant_type]:
                        if not pd.api.types.is_numeric_dtype(plant_df[feature]):
                            plant_df = plant_df.drop(columns=[feature])
                            logging.warning(f"Dropped non-numeric feature {feature} in {plant_type}")

                    # Append new data to the deque (sliding window)
                    data_entries = data_store[plant_type]['data']
                    data_entries.extend(plant_df.to_dict('records'))

        except Exception as e:
            logging.error(f"Error processing batch {batch_id}: {e}")

    # Apply processing to each micro-batch
    query = df_parsed.writeStream \
        .trigger(processingTime='1 second') \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

def perform_outlier_detection():
    """
    Perform outlier detection using Isolation Forest on a sliding window.
    """
    while True:
        with data_lock:
            for plant_type in plant_types:
                data_entries = data_store[plant_type]['data']
                if len(data_entries) < 50:
                    continue  # Need sufficient data

                data_df = pd.DataFrame(list(data_entries))
                features = plant_features[plant_type]
                data_features = data_df[features].astype(float)

                # Algorithm Explanation:
                # Isolation Forest is an unsupervised learning algorithm for anomaly detection
                # that isolates anomalies instead of profiling normal data points. It works well
                # with high-dimensional data and is effective in detecting anomalies in the presence
                # of concept drift and seasonal variations due to its tree-based structure.

                # Fit Isolation Forest on sliding window
                isolation_forest = IsolationForest(contamination=0.05, random_state=42)
                outlier_labels = isolation_forest.fit_predict(data_features)

                # Identify outliers
                outlier_indices = np.where(outlier_labels == -1)[0]
                if len(outlier_indices) > 0:
                    outliers_df = data_df.iloc[outlier_indices]
                    data_store[plant_type]['outliers'] = outliers_df
                    logging.info(f"Detected {len(outlier_indices)} outliers for {plant_type}")
                else:
                    data_store[plant_type]['outliers'] = pd.DataFrame()

                # Limit outliers DataFrame size
                max_outliers = 100
                if len(data_store[plant_type]['outliers']) > max_outliers:
                    data_store[plant_type]['outliers'] = data_store[plant_type]['outliers'].iloc[-max_outliers:]

        time.sleep(5)  # Wait before next detection cycle

# ============================================================================
# UI / THEME CONFIGURATION
# ============================================================================
# Initialize Dash app
external_stylesheets = [dbc.themes.DARKLY]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Cobblestone Energy Efficient Data Stream Anomaly Detection"

# ---- Palette: dark operations-console theme, restrained & semantic ----
bg_app = '#0A0C0F'            # page background
bg_panel = '#111418'          # card background
bg_panel_header = '#0D0F13'   # header strip / table header background
bg_chart = '#0D1013'          # plot area background (slightly recessed vs. card)
border_color = '#20252C'      # hairline borders
text_primary = '#E4E6E9'      # headings / primary text
text_secondary = '#9BA1AA'    # axis labels, table text, body copy
text_tertiary = '#5C636D'     # placeholders, faint captions
grid_line = '#1A1E24'         # chart gridlines

status_live = '#4C9E6B'       # muted green - "live" indicator
status_alert = '#D1544A'      # muted red - anomalies, used everywhere anomalies appear

# One muted accent per plant type, used only as a small identifier
# (left border + header mark) rather than a decorative wash.
plant_theme = {
    'Gas Plant': {
        'accent': '#C9813F',
        'subtitle': 'Combustion & fuel efficiency monitoring',
    },
    'Wind Farm': {
        'accent': '#4C9E85',
        'subtitle': 'Turbine performance & wind conditions',
    },
    'Solar Farm': {
        'accent': '#C9A227',
        'subtitle': 'Irradiance & panel thermal behavior',
    },
    'Hydroelectric Plant': {
        'accent': '#3E7CA6',
        'subtitle': 'Flow rate & turbine rotation dynamics',
    },
}

# Every metric keeps the same colour on every chart it appears on, so e.g.
# "power_output" reads the same way across all four panels.
FEATURE_COLORS = {
    'power_output':          '#4E79A7',
    'demand':                '#F28E2B',
    'fuel_consumption':      '#E15759',
    'emissions':             '#B07AA1',
    'wind_speed':            '#59A14F',
    'turbine_efficiency':    '#76B7B2',
    'solar_radiation':       '#EDC948',
    'panel_temperature':     '#9C755F',
    'water_flow_rate':       '#59A14F',
    'turbine_rotation_speed': '#76B7B2',
}

outlier_marker_color = status_alert  # one consistent colour for every anomaly marker

FONT_BODY = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
FONT_MONO = "ui-monospace, 'SF Mono', 'Roboto Mono', Consolas, 'Liberation Mono', Menlo, monospace"
FONT_HEADING = FONT_BODY  # one type family; weight/size carry the hierarchy instead

# Custom index string: global font, background, scrollbar, card + table styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            html, body {
                background-color: ''' + bg_app + ''' !important;
                font-family: ''' + FONT_BODY + ''';
            }

            ::-webkit-scrollbar { width: 10px; height: 10px; }
            ::-webkit-scrollbar-track { background: ''' + bg_app + '''; }
            ::-webkit-scrollbar-thumb { background: ''' + border_color + '''; border-radius: 6px; }
            ::-webkit-scrollbar-thumb:hover { background: #2C333C; }

            :focus-visible {
                outline: 2px solid #4E79A7;
                outline-offset: 2px;
            }

            .app-navbar {
                background-color: ''' + bg_panel_header + ''';
                border-bottom: 1px solid ''' + border_color + ''';
            }

            .logo-mark {
                width: 30px;
                height: 30px;
                border: 1px solid ''' + border_color + ''';
                border-radius: 6px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-family: ''' + FONT_MONO + ''';
                font-weight: 600;
                font-size: 12px;
                color: ''' + text_primary + ''';
                background-color: ''' + bg_panel + ''';
                flex-shrink: 0;
            }

            .live-dot {
                width: 7px;
                height: 7px;
                border-radius: 50%;
                background-color: ''' + status_live + ''';
                display: inline-block;
                flex-shrink: 0;
            }
            @media (prefers-reduced-motion: no-preference) {
                .live-dot { animation: pulse-dot 2.2s ease-in-out infinite; }
            }
            @keyframes pulse-dot {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.35; }
            }

            .plant-card {
                background-color: ''' + bg_panel + ''';
                border: 1px solid ''' + border_color + ''';
                border-radius: 8px;
                overflow: hidden;
                transition: border-color 0.15s ease;
            }
            .plant-card:hover {
                border-color: #333B46;
            }
            .plant-card-header {
                background-color: ''' + bg_panel_header + ''';
                border-bottom: 1px solid ''' + border_color + ''';
                padding: 14px 18px;
            }
            .plant-card-body { padding: 16px 18px 20px 18px; }

            .category-mark {
                width: 9px;
                height: 9px;
                border-radius: 2px;
                display: inline-block;
                flex-shrink: 0;
            }

            .section-label {
                font-size: 11px;
                font-weight: 600;
                letter-spacing: 0.06em;
                text-transform: uppercase;
                color: ''' + text_secondary + ''';
                padding-bottom: 8px;
                border-bottom: 1px solid ''' + border_color + ''';
                margin: 20px 0 12px 0;
                display: flex;
                align-items: center;
                gap: 8px;
            }

            .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner table {
                font-family: ''' + FONT_MONO + ''' !important;
            }

            .dash-table-container .previous-page,
            .dash-table-container .next-page,
            .dash-table-container .first-page,
            .dash-table-container .last-page {
                background-color: ''' + bg_panel_header + ''' !important;
                border: 1px solid ''' + border_color + ''' !important;
                color: ''' + text_secondary + ''' !important;
            }
            .dash-table-container input.current-page {
                background-color: ''' + bg_panel_header + ''' !important;
                border: 1px solid ''' + border_color + ''' !important;
                color: ''' + text_primary + ''' !important;
                font-family: ''' + FONT_MONO + ''' !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''


def _empty_figure(message='Waiting for stream data\u2026'):
    """Themed placeholder shown before the first batch of data arrives (or on error)."""
    return go.Figure(
        layout=go.Layout(
            plot_bgcolor=bg_chart,
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            margin=dict(l=20, r=20, t=20, b=20),
            annotations=[dict(
                text=message,
                showarrow=False,
                font=dict(color=text_tertiary, family=FONT_BODY, size=12),
            )],
        )
    )


def build_plant_card(plant_type):
    """
    Build a themed dashboard card (graph + anomaly table) for a given plant type.
    Purely presentational - wraps the same dcc.Graph / dash_table.DataTable
    components the callback already targets by id.
    """
    theme = plant_theme[plant_type]
    slug = plant_type.lower().replace(' ', '-')
    features = plant_features[plant_type]

    return dbc.Col(
        html.Div(
            [
                # Card header: small category mark + title. No icon glyphs.
                html.Div(
                    html.Div(
                        [
                            html.Span(
                                className='category-mark',
                                style={'backgroundColor': theme['accent'], 'marginRight': '10px'}
                            ),
                            html.Div(
                                [
                                    html.Span(
                                        plant_type,
                                        style={
                                            'fontFamily': FONT_HEADING,
                                            'fontWeight': '600',
                                            'fontSize': '15px',
                                            'color': text_primary,
                                            'display': 'block',
                                            'lineHeight': '1.3',
                                        }
                                    ),
                                    html.Span(
                                        theme['subtitle'],
                                        style={
                                            'fontFamily': FONT_BODY,
                                            'fontSize': '12px',
                                            'color': text_secondary,
                                        }
                                    ),
                                ],
                                style={'display': 'inline-block', 'verticalAlign': 'middle'}
                            ),
                        ],
                        style={'display': 'flex', 'alignItems': 'center'}
                    ),
                    className='plant-card-header'
                ),

                # Card body: graph + anomaly table
                html.Div(
                    [
                        dcc.Graph(id=f'{slug}-graph', animate=False, config={'displayModeBar': False}),
                        html.Div(
                            [
                                html.Span(
                                    className='category-mark',
                                    style={'backgroundColor': status_alert, 'borderRadius': '50%'}
                                ),
                                html.Span("Detected Anomalies"),
                            ],
                            className='section-label'
                        ),
                        dash_table.DataTable(
                            id=f'{slug}-table',
                            columns=[{'name': f.replace('_', ' ').title(), 'id': f} for f in features],
                            style_table={'overflowX': 'auto', 'borderRadius': '6px', 'border': f'1px solid {border_color}'},
                            style_cell={
                                'textAlign': 'left',
                                'minWidth': '100px',
                                'backgroundColor': bg_panel,
                                'color': text_primary,
                                'border': f'1px solid {border_color}',
                                'fontFamily': FONT_MONO,
                                'fontSize': '12px',
                                'padding': '8px 10px',
                            },
                            style_header={
                                'backgroundColor': bg_panel_header,
                                'color': text_secondary,
                                'fontWeight': '600',
                                'fontFamily': FONT_BODY,
                                'fontSize': '11px',
                                'letterSpacing': '0.04em',
                                'textTransform': 'uppercase',
                                'border': f'1px solid {border_color}',
                            },
                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': bg_panel_header,
                                }
                            ],
                            style_as_list_view=True,
                            page_size=5,
                        ),
                    ],
                    className='plant-card-body'
                ),
            ],
            className='plant-card',
            style={'borderLeft': f'3px solid {theme["accent"]}'}
        ),
        xs=12, xl=6,
        style={'marginBottom': '24px'}
    )


# Define app layout
app.layout = html.Div(
    [
        # ---- Navbar ----
        html.Div(
            dbc.Container(
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                [
                                    html.Div("CE", className='logo-mark'),
                                    html.Div(
                                        [
                                            html.Span(
                                                "Cobblestone Energy",
                                                style={
                                                    'fontFamily': FONT_HEADING,
                                                    'fontWeight': '700',
                                                    'fontSize': '18px',
                                                    'color': text_primary,
                                                    'display': 'block',
                                                    'lineHeight': '1.2'
                                                }
                                            ),
                                            html.Span(
                                                "Efficient Data Stream Anomaly Detection",
                                                style={
                                                    'fontFamily': FONT_BODY,
                                                    'fontSize': '12px',
                                                    'color': text_secondary,
                                                    'letterSpacing': '0.01em'
                                                }
                                            ),
                                        ],
                                        style={'marginLeft': '12px'}
                                    ),
                                ],
                                style={'display': 'flex', 'alignItems': 'center', 'padding': '14px 0'}
                            ),
                            width='auto'
                        ),
                        dbc.Col(
                            html.Div(
                                [
                                    html.Span(className='live-dot'),
                                    html.Span(
                                        "Live",
                                        style={
                                            'fontFamily': FONT_MONO,
                                            'fontSize': '11px',
                                            'letterSpacing': '0.06em',
                                            'textTransform': 'uppercase',
                                            'color': text_secondary,
                                            'marginLeft': '8px',
                                        }
                                    ),
                                ],
                                style={'display': 'flex', 'alignItems': 'center'}
                            ),
                            width='auto',
                            className='ms-auto d-flex align-items-center'
                        ),
                    ],
                    justify='between',
                    align='center',
                    className='g-0'
                ),
                fluid=True
            ),
            className='app-navbar',
            style={'marginBottom': '28px', 'position': 'sticky', 'top': 0, 'zIndex': 999}
        ),

        # ---- Plant cards grid ----
        dbc.Container(
            dbc.Row(
                [build_plant_card(pt) for pt in plant_types],
                className='g-4'
            ),
            fluid=True
        ),

        dcc.Interval(
            id='graph-update',
            interval=1 * 1000,  # Update every second
            n_intervals=0
        ),

        # ---- Footer ----
        html.Footer(
            dbc.Container(
                html.P(
                    "Real-Time Energy Efficient Data Stream Anomaly Detection \u2014 by Vinay",
                    className="text-center",
                    style={'color': text_tertiary, 'padding': '18px', 'fontFamily': FONT_BODY, 'fontSize': '12px', 'margin': 0}
                )
            ),
            style={'borderTop': f'1px solid {border_color}', 'marginTop': '32px'}
        )
    ],
    style={'backgroundColor': bg_app, 'minHeight': '100vh', 'paddingBottom': '20px'}
)

@app.callback(
    [
        Output('gas-plant-graph', 'figure'),
        Output('gas-plant-table', 'data'),
        Output('wind-farm-graph', 'figure'),
        Output('wind-farm-table', 'data'),
        Output('solar-farm-graph', 'figure'),
        Output('solar-farm-table', 'data'),
        Output('hydroelectric-plant-graph', 'figure'),
        Output('hydroelectric-plant-table', 'data'),
    ],
    [Input('graph-update', 'n_intervals')]
)
def update_graphs(n):
    """
    Update the graphs and tables in the Dash app with the latest data.

    Args:
        n (int): The number of intervals passed.

    Returns:
        tuple: Figures and table data for each power plant type.
    """
    try:
        with data_lock:
            figures = []
            table_data = []
            for plant_type in plant_types:
                data_entries = data_store[plant_type]['data']
                if not data_entries:
                    figures.append(_empty_figure())
                    table_data.append([])
                    continue

                data_df = pd.DataFrame(list(data_entries))
                outliers_df = data_store[plant_type]['outliers']

                # Plot time series
                features = plant_features[plant_type]
                data_traces = []

                for i, feature in enumerate(features):
                    data_traces.append(
                        go.Scatter(
                            x=data_df['timestamp'],
                            y=data_df[feature],
                            mode='lines',
                            name=f'{feature.replace("_", " ").title()}',
                            line=dict(width=2, color=FEATURE_COLORS.get(feature, text_secondary)),
                            hoverinfo='text',
                            hovertext=[
                                f'Time: {t}<br>{feature.replace("_", " ").title()}: {v:.2f}'
                                for t, v in zip(data_df['timestamp'], data_df[feature])
                            ]
                        )
                    )

                # Add outlier markers. All four series are still plotted individually
                # (same detection results as before) but share a single legend entry
                # instead of four duplicate "Outliers (...)" rows.
                if not outliers_df.empty:
                    for i, feature in enumerate(features):
                        data_traces.append(
                            go.Scatter(
                                x=outliers_df['timestamp'],
                                y=outliers_df[feature],
                                mode='markers',
                                name='Anomaly',
                                legendgroup='anomalies',
                                showlegend=(i == 0),
                                marker=dict(color=outlier_marker_color, size=8, symbol='x'),
                                hoverinfo='text',
                                hovertext=[
                                    f'Time: {t}<br>{feature.replace("_", " ").title()}: {v:.2f}'
                                    for t, v in zip(outliers_df['timestamp'], outliers_df[feature])
                                ]
                            )
                        )

                layout = go.Layout(
                    xaxis=dict(
                        color=text_secondary,
                        gridcolor=grid_line,
                        zerolinecolor=grid_line,
                        showline=True,
                        linecolor=border_color,
                        tickfont=dict(size=10, color=text_secondary, family=FONT_MONO),
                    ),
                    yaxis=dict(
                        color=text_secondary,
                        gridcolor=grid_line,
                        zerolinecolor=grid_line,
                        showline=True,
                        linecolor=border_color,
                        tickfont=dict(size=10, color=text_secondary, family=FONT_MONO),
                    ),
                    legend=dict(
                        orientation='h',
                        yanchor='bottom', y=1.02,
                        xanchor='left', x=0,
                        font=dict(color=text_secondary, size=10, family=FONT_BODY),
                        bgcolor='rgba(0,0,0,0)',
                    ),
                    hovermode='closest',
                    hoverlabel=dict(
                        bgcolor=bg_panel_header,
                        bordercolor=border_color,
                        font=dict(color=text_primary, family=FONT_BODY, size=11)
                    ),
                    plot_bgcolor=bg_chart,
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color=text_secondary, family=FONT_BODY),
                    margin=dict(l=44, r=16, t=48, b=32),
                )

                figures.append({'data': data_traces, 'layout': layout})

                # Prepare table data
                if not outliers_df.empty:
                    table_entries = outliers_df[features + ['timestamp']].to_dict('records')
                else:
                    table_entries = []

                table_data.append(table_entries)

        return (
            figures[0], table_data[0],
            figures[1], table_data[1],
            figures[2], table_data[2],
            figures[3], table_data[3],
        )
    except Exception as e:
        logging.error(f"Error in update_graphs: {e}")
        empty_figure = _empty_figure('Something went wrong loading this stream')
        return empty_figure, [], empty_figure, [], empty_figure, [], empty_figure, []

if __name__ == '__main__':
    # Start Spark Streaming in a separate thread
    streaming_thread = threading.Thread(target=start_spark_streaming, daemon=True)
    streaming_thread.start()

    # Start Outlier Detection in a separate thread
    outlier_thread = threading.Thread(target=perform_outlier_detection, daemon=True)
    outlier_thread.start()

    # Run the Dash app
    app.run(debug=False, host='0.0.0.0', port=8050)