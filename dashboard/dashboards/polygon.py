from dashboard.queries.polygon import get
import plotly.io as pio
import plotly.express as px
from dash import html, dcc

pio.templates.default = "plotly_dark"

df = get()
fig = px.line(df, x="date", y="close")
element = html.Div([
    html.H1(["Polygon Stock Close Price Prediction"], style={"textAlign": "center"}),
    dcc.Graph(id="polygon", figure=fig)
])