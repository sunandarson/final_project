#!python3

import os
import json
import pandas

from kafka import KafkaConsumer
from sqlalchemy import create_engine


if __name__ == "__main__":
    print("starting the consumer")
    path = os.getcwd()+"/"

    # connect database
    try:
        engine = create_engine(
            'postgresql://postgres:admin@172.31.224.1:5432/dwh_digitalskola')
        print(f"[INFO] Successfully Connect Database .....")
    except:
        print(f"[INFO] Error Connect Database .....")

    # connect kafka server
    try:
        consumer = KafkaConsumer(
            "final-project", bootstrap_servers='localhost')
        print(f"[INFO] Successfully Connect Kafka Server .....")
    except:
        print(f"[INFO] Error Connect Kafka Server .....")

    # read message from topic kafka server
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")

        # insert database
        df = pandas.DataFrame(data, index=[0])
        df.to_sql('dim_search_log', engine, if_exists='append', index=False)

    # transform database


def transformStream(df):
    df = df \
        .groupby(['date_search', 'product_search']) \
        .agg({'id_search': ['count']}).reset_index()

    return df.head()
