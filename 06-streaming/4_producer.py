import csv
from datetime import datetime
import gzip
import json
from time import time

from kafka import KafkaProducer


COLUMNS = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def main():
    dt_start = datetime(year=2019, month=10, day=1)
    dt_end = datetime(year=2019, month=11, day=1)

    t0 = time()

    producer = KafkaProducer(
        bootstrap_servers = ['redpanda-1:29092'],
        value_serializer = json_serializer
    )

    with gzip.open('/home/user/data/green_tripdata_2019-10.csv.gz', 'rt') as f:
        reader = csv.DictReader(f)

        for row in reader:
            if datetime.strptime(row['lpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S') >= dt_start \
                and datetime.strptime(row['lpep_dropoff_datetime'], '%Y-%m-%d %H:%M:%S') < dt_end:
                
                data = {col.lower(): (row[col] if row[col] != '' else None) for col in columns}
                producer.send('green-trips', value=data)

    producer.flush()
    producer.close()

    t1 = time()
    took = t1 - t0
    print(took)


if __name__ == '__main__':
    main()