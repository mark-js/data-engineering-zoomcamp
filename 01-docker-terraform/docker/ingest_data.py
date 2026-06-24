import argparse
from urllib.request import urlretrieve

import pandas as pd
from sqlalchemy import create_engine



def prepare_data(df):
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])


def create_table(df, table_name, engine):
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')


def ingest_data(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, if_exists='append')
    

def main():
    parser = argparse.ArgumentParser(description='Ingest data')
    group = parser.add_mutually_exclusive_group(required=True)
    
    group.add_argument('-u', '--url', help='url of file')
    group.add_argument('-f', '--filename', help='filename')
    parser.add_argument('-i', '--ip', required=True, help='ip address')
    parser.add_argument('-d', '--dbname', required=True, help='database name')
    parser.add_argument('-t', '--tablename', required=True, help='table name')

    args = parser.parse_args()
    if args.url:
        filename = args.url.split('/')[-1]
        urlretrieve(args.url, f'data/{filename}')
    else: 
        filename = args.filename

    df = pd.read_csv(f'data/{filename}', nrows=10)
    df_iter = pd.read_csv(f'data/{filename}', iterator=True, chunksize=100000)
    engine = create_engine(f'postgresql://root:root@{args.ip}:5432/{args.dbname}') 

    # prepare_data(df)
    print(pd.io.sql.get_schema(df, args.tablename))
    create_table(df, args.tablename, engine)

    n = 0
    for df in df_iter:
        n += 1
        # prepare_data(df)
        ingest_data(df, args.tablename, engine)
        print(f'inserting chunk {n} ...')

if __name__ == '__main__':
    main()