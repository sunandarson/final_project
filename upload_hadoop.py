#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    # connect db warehouse
    conn_dwh, engine_dwh = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    # connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    # connect hadoop
    conf = connection.config('hadoop')
    client = connection.hadoop_conn(conf)

    # query extract db source
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query_data.sql', 'r'
        ).read(), strip_comments=True).strip()

    # query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_data.sql', 'r'
        ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)
        df = df.dropna(how='any', axis=0)

    # upload hadoop
        with client.write(f"/digitalskola/finalproject/dim_ct_{filetime}.csv", encoding='utf-8') as writer:
            df.to_csv(writer, index=False)
        print(f"[INFO] Upload Data in HADOOP Success .....")

    except:
        print(f"[INFO] Service ETL is Failed .....")
