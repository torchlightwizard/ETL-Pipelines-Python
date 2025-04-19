from dashboard.queries.open_meteo import get
from dash import html

df = get()
element = html.Div([
    html.H1(["Open Meteo Rain Prediction"], style={"textAlign": "center"}),

    html.Div([
        html.Div([
            html.Div("Date", style={
                "fontWeight": "bold",
                "color": "#aaa"
            }),
            html.Div(df["date"].iloc[0], style={
                "color": "#fff"
            })
        ], style={
            "display": "flex",
            "justifyContent": "space-between",
            "padding": "0.5rem 0",
            "borderBottom": "1px solid #333"
        }),

        html.Div([
            html.Div("Rain", style={
                "fontWeight": "bold",
                "color": "#aaa"
            }),
            html.Div("Yes" if df["rain"].iloc[0] else "No", style={
                "color": "#fff" if df["rain"].iloc[0] else "#ff6347"  # Red if No rain
            })
        ], style={
            "display": "flex",
            "justifyContent": "space-between",
            "padding": "0.5rem 0"
        }),

    ], style={
        "backgroundColor": "#1e1e1e",
        "color": "#e0e0e0",
        "borderRadius": "10px",
        "padding": "1rem",
        "boxShadow": "0 4px 12px rgba(0, 0, 0, 0.4)",
        "width": "300px",
        "margin": "1rem auto"
    })
])