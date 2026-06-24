import json
from time import time

from kafka import KafkaProducer
import polars as pl


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def main():
    t0 = time()

    producer = KafkaProducer(
        bootstrap_servers = ['redpanda-1:29092'],
        value_serializer = json_serializer
    )

    df = pl.read_csv('/home/user/data/green_tripdata_2019-10.csv.gz', try_parse_dates=True) \
        .sort('lpep_dropoff_datetime') \
        .select(
            (pl.col('lpep_pickup_datetime').dt.strftime('%Y-%m-%d %H:%M:%S')).alias('lpep_pickup_datetime'),
            (pl.col('lpep_dropoff_datetime').dt.strftime('%Y-%m-%d %H:%M:%S')).alias('lpep_dropoff_datetime'),
            (pl.col('PULocationID').alias('pulocationid')),
            (pl.col('DOLocationID').alias('dolocationid')),
            (pl.col('passenger_count')),
            (pl.col('trip_distance')),
            (pl.col('tip_amount'))
        )

    data = df.to_dicts()

    for row in data:
        producer.send('green-trips-sorted', value=row)

    producer.flush()
    producer.close()
        
    t1 = time()
    took = t1 - t0
    print(took)


if __name__ == '__main__':
    main()