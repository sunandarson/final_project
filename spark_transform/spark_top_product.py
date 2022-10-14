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
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    # connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="ETL-FinalProject", config=conf)

    # query extract db source
    path_query = os.path.normpath(os.getcwd() + os.sep + os.pardir)+'/query/'
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
        dfclean = df.dropna(how='any', axis=0)

        # directory local
        path_directory = os.path.normpath(
            os.getcwd() + os.sep + os.pardir)+'/local/'

        # spark processing
        sparkdf = spark.createDataFrame(dfclean)
        sparkdf.groupBy('product') \
            .agg((func.count('id_transaction'))
                 .alias('jumlah_terjual')) \
            .toPandas().sort_values(by='jumlah_terjual', ascending=False) \
            .to_csv(path_directory+'dim_top_product.csv', index=False)\
            .to_sql(f"dim_product_top", con=engine_dwh, index=False)

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
