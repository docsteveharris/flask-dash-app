import os

from .dash import Dash
from dash import dcc, html
from dash.dependencies import Input, Output, State

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime

from pathlib import Path
from sqlalchemy import create_engine
from .setup import load_env_vars, make_emap_engine
from .wrangle import gen_query_recent_messages

load_env_vars()
ids_engine = make_emap_engine('ids')

stop = round_time(datetime.datetime.now(),
                  date_delta=datetime.timedelta(seconds=5))
start = stop - datetime.timedelta(hours=1)
query = gen_query_recent_messages(start.isoformat(), stop.isoformat())


df = pd.read_sql_query(query, ids_engine)
foo = str(df.loc[1])

app_layout = html.Div(
    children=[html.H1(children=f"Hello Dash {foo}")], className="container-fluid"
)


def init_dash(server):
    dash_app = Dash(server=server, routes_pathname_prefix="/demo/",)
    dash_app.layout = app_layout
    return dash_app.server


if __name__ == "__main__":
    app = Dash(__name__)
    app.run_server(debug=True)
