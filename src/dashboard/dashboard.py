from src.step.step import BaseStep
from dash import Dash, html
from utils.dashboard.html import get



class Dashboard (BaseStep):
    def __init__ (self):
        super().__init__()
        elements = get()
        divs = [html.Div([el], className="card") for el in elements]

        self.app = Dash()
        self.app.layout = html.Div(divs, className="dashboard")
    
    
    
    def run(self, data=None):
        self.app.run(debug=True)