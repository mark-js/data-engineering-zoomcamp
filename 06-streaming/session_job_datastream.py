import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")


def show(ds, env):
    ds.print()
    env.execute()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(interval=10 * 1000)
    env.set_parallelism(1)

    type_info_source = Types.ROW_NAMED(
        [
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'pulocationid',
            'dolocationid',
            'passenger_count',
            'trip_distance',
            'tip_amount',
        ],
        [
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE()
        ]
    )

    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(type_info_source) \
        .build()

    kafka_source = KafkaSource.builder() \
        .set_topics('green-trips') \
        .set_bootstrap_servers('redpanda-1:29092') \
        .set_group_id('test_group_13') \
        .set_value_only_deserializer(deserialization_schema) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_minutes(1))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[1]  # We treat the second tuple element as the event-time (ms).
        )
    )

    type_info_sink = Types.ROW_NAMED(
        [
            'window_start',
            'window_end',
            'pulocationid',
            'num_hits'
        ],
        [
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.INT(),
            Types.LONG()
        ]
    )

    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name='kafka_source'
    )

    show(ds, env)


if __name__ == '__main__':
    main()