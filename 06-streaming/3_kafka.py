import json

from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def main():
    server = 'redpanda-1:29092'

    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer
    )

    print(producer.bootstrap_connected())


if __name__ == '__main__':
    main()