#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os 

def ingest_data(params) :
    user = params.user 
    password = params.password 
    host = params.host 
    port = params.port 
    db = params.db 
    table_name = params.table_name 
    url = params.url

    csv_name = 'output.csv'

    splitted_url = url.split('.')

    if(splitted_url[-1]=='gz') :
        os.system(f"curl {url} -L --output {csv_name}.gz")
        os.system(f"gzip {csv_name}.gz -d")
    elif(splitted_url[-1]=='csv') :
        os.system(f"curl {url} -L --output {csv_name}")
        os.system(f"ls -a")
    else :
        print("ERROR")
        return 0 
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    while True:
        try:
            df = next(df_iter)
            if("green" in  url ) :
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
        except StopIteration:
            break

if __name__ == '__main__' :
    urls = [
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz",
        "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    ]
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for host')
    parser.add_argument('--password', help='password for host')
    parser.add_argument('--host', help='host')
    parser.add_argument('--port', help='port for host')
    parser.add_argument('--db', help='db')
    parser.add_argument('--table_name', help='table_name')
    parser.add_argument('--url', help='url')

    args = parser.parse_args()
    
    for url in urls :
        args.url = url
        if "green" in url :
            args.table_name = "green_taxi_data"
        else : 
            args.table_name = "taxi_zone_lookup"
        
        ingest_data(args)