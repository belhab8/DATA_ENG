import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url


    parquet_name = url.split('/')[-1]
    os.system(f"curl -o {parquet_name} {url}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    data = pq.ParquetFile(parquet_name)
    for batch in data.iter_batches(batch_size=100000):
        df = batch.to_pandas()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='append')

        df.to_sql(name=table_name, con=engine, if_exists="append")
        print("New batch appended")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    # user, password, host, port, database name, table name, url of the csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help="name of the postgres table where we're writing")
    parser.add_argument('--url', help='url of the csv file to ingest')

    args = parser.parse_args()
    main(args)