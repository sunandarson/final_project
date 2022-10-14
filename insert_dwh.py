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

    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    # # connect spark
    # conf = connection.config('spark')
    # spark = connection.spark_conn(app="ETL-FinalProject",config=conf)

    #query extract db source
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query_data.sql','r'
            ).read(), strip_comments=True).strip()

    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_data.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)
        #proses cleaning, menghilangkan data null
        dfclean = df.dropna(how='any',axis=0) 
        
        #upload local
        path = os.getcwd()
        directory = path+'/'+'local'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
            dfclean.to_csv(f"{directory}dim_customer_transact_{filetime}.csv", index=False)
            print(f"[INFO] Upload Data in LOCAL Success .....")

        #insert dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()
        dfclean.to_sql('dim_customer_transaction', engine_dwh,\
            if_exists='append', index=False)
        print(f"[INFO] Insert DWH Success .....")
        
        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    

    