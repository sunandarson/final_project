#!python3

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

if __name__ == "__main__":
    username = 'postgres'
    password = 'admin'
    database = 'digitalskola'
    ip = '172.29.96.1'

    try:
        engine = create_engine(
            f"postgresql://{username}:{password}@{ip}:5432/{database}")
        print(f"[SUCCESS] Connect PostgreSQL...")
    except:
        print(f"[FAILED] Can't Connect PostgreSQL...")

    list_filename = ['customer', 'product', 'transaction']
    for file in list_filename:
        pd.read_csv(f"bigdata_{file}.csv").to_sql(
            f"bigdata_{file}", con=engine)
        print(f"[SUCCESS] Dump File {file}...")
