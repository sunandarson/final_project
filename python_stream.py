#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
from sqlite3 import Row
from pyspark.sql.functions import year
from pyspark.sql import functions as func
from pyspark.sql import functions as F
from pyspark.sql import types as T

import os
import json
from ossaudiodev import SNDCTL_SYNTH_MEMAVL
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
    conf = connection.config('warehouse')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    # query extract db source
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'search_log.sql', 'r'
        ).read(), strip_comments=True).strip()

    # try:
    #     print(f"[INFO] Service ETL is Running .....")
    #     df = pd.read_sql(query, engine)
    #     #proses cleaning, menghilangkan data null
    #     dfclean = df.dropna(how='any',axis=0)
    #     dfclean.to_sql().groupby('date_search').agg('date_search')\
    #         .sort_values(by='date_search', ascending=False)

    #     #upload local
    #     path = os.getcwd()
    #     directory = path+'/'+'local'+'/'
    #     dfclean.to_csv( directory +'dim_search.csv', index=False)

    #     print(f"[INFO] Upload Data in LOCAL Success .....")

    #     print(f"[INFO] Service ETL is Success .....")
    # except:
    #     print(f"[INFO] Service ETL is Failed .....")

    df = pd.read_sql(query, engine)
    # proses cleaning, menghilangkan data null
    dfclean = df.dropna(how='any', axis=0)
    dfsearch = dfclean.groupby(['date_search', 'id_search'])[
        'id_search'].count()
    # upload local
    path = os.getcwd()
    directory = path+'/'+'local'+'/'
    dfsearch.to_csv(directory + 'dim_search_top.csv', index=False)
