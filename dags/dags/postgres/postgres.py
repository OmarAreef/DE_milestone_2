import psycopg2
from sqlalchemy import create_engine
import pandas as pd

import os
def tables_to_postgres():
    path_to_table = os.path.abspath(os.path.join("dags/2020_Accidents_UK.csv"))
    path_to_lookup = os.path.abspath(os.path.join("dags/2020_Accidents_UK_transformed.csv"))
    df = pd.read_csv(path_to_table)
    lookup = pd.read_csv(path_to_lookup)
    conn = create_engine('postgresql://root:password@pgdatabase:5432/milestone2_etl')
    # conn = psycopg2.connect(dbname="de", user="postgres", password="123")
    print(conn)
    df.to_sql('UK_Accidents_2020', con = conn, if_exists = 'replace', index = False)
    lookup.to_sql('lookup_table', con = conn, if_exists = 'replace', index = False)
