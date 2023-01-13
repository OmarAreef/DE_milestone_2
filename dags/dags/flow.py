from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import matplotlib.pyplot as plt
import seaborn as sb
import plotly.express as px
import json
from plotly.tools import mpl_to_plotly
import datetime
from ETLscripts.prepare import prepare_and_clean_df
from ETLscripts.transformation import perform_enconding
from ETLscripts.transformation import add_columns
from postgres.postgres import tables_to_postgres


def task1_ETL():
    path_to_csv = os.path.abspath(os.path.join("dags/2020_Accidents_UK.csv"))
    df = pd.read_csv(path_to_csv)
    df = prepare_and_clean_df(df)
    df = perform_enconding(df)
    df = add_columns(df)
    path_to_new_csv = os.path.abspath(os.path.join("dags/2020_Accidents_UK_transformed.csv"))
    df.to_csv(path_to_new_csv)


def task2_extract_figures():
    path_to_new_csv = os.path.abspath(os.path.join("dags/2020_Accidents_UK_transformed.csv"))
    df = pd.read_csv(path_to_new_csv)
    fig = px.scatter(x=df["longitude"], y=df["latitude"])
    fig.update_layout(xaxis_title="latitude", yaxis_title="longitude")

    fig2 = px.histogram(df, x="day_of_week", title="Number of accidents per day")
    fig2.update_layout(xaxis_title="Day", yaxis_title="Count")

    def get_hour(x: str):
        return x.split(":")[0]

    df["time_hour"] = df["time"].apply(get_hour)
    fig3 = px.histogram(df, x="time_hour", title="Number of accidents per Time hour")
    fig3.update_layout(xaxis_title="Hour", yaxis_title="Count")

    fig4 = px.histogram(df, x="speed_limit", title="Number of accidents per speed limit")
    fig4.update_layout(xaxis_title="speed limit", yaxis_title="Count")

    fig5 = px.histogram(
        df, x="weather_conditions", title="Number of accidents per weather condition"
    )
    fig5.update_layout(xaxis_title="Weather Condition", yaxis_title="Number of Accidents")
    app = dash.Dash(__name__)
    app.layout = html.Div(
        children=[
            html.H1(children="UK Accidents 2020 Dashboard"),
            html.H2(children="Location distribution of accidents"),
            dcc.Graph(figure=fig),
            html.H2(children="Distribution of accidents accross the day of the week"),
            dcc.Graph(figure=fig2),
            html.H2(children="Distribution of accidents accross the hour per day"),
            dcc.Graph(figure=fig3),
            html.H2(children="Distribution of accidents accross speed limit"),
            dcc.Graph(figure=fig4),
            html.H2(children="Distribution of accidents accross weather conditions"),
            dcc.Graph(figure=fig5),
        ]
    )

    app.run_server(debug=False)


def task3_postgres():
    tables_to_postgres()


task2_extract_figures()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2023, 1, 6),
}
dag = DAG(
    "UK_Accidents_2020-DAG",
    default_args=default_args,
    description="data ETL in UK car accidents in 2020",
    schedule="@once",
)

t1 = PythonOperator(
    task_id="extract_clean_data",
    python_callable=task1_ETL,
    dag=dag,
)

t2 = PythonOperator(
    task_id="Load_to_postgres",
    python_callable=task3_postgres,
    dag=dag,
)

t3 = PythonOperator(
    task_id="Extract_figures",
    python_callable=task2_extract_figures,
    dag=dag,
)



t1 >> t2 >> t3
