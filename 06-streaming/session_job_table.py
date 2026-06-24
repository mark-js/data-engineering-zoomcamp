import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, FormatDescriptor, DataTypes
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit


def create_source_schema() -> Schema:
    return Schema.new_builder() \
        .column('lpep_pickup_datetime', DataTypes.STRING()) \
        .column('lpep_dropoff_datetime', DataTypes.STRING()) \
        .column('pulocationid', DataTypes.INT()) \
        .column('dolocationid', DataTypes.INT()) \
        .column('passenger_count', DataTypes.INT()) \
        .column('trip_distance', DataTypes.DOUBLE()) \
        .column('tip_amount', DataTypes.DOUBLE()) \
        .column_by_expression('watermark_strategy', col('lpep_pickup_datetime').to_timestamp) \
        .watermark('watermark_strategy', 'watermark_strategy - INTERVAL 7 DAYS') \
        .build()


def create_kafka_source(t_env:StreamTableEnvironment, source_schema:Schema) -> str:
    path_source = 'kafka_source'
    source_table_descriptor = TableDescriptor.for_connector('kafka') \
        .schema(source_schema) \
        .option('topic', 'green-trips') \
        .option('properties.bootstrap.servers', 'redpanda-1:29092') \
        .option('properties.group.id', 'test_group_0') \
        .option('scan.startup.mode', 'earliest-offset') \
        .format(FormatDescriptor.for_format('json')
            .option('fail-on-missing-field', 'false')
            .option('ignore-parse-errors', 'true')
            .build()) \
        .build()
    t_env.create_temporary_table(
        path=path_source,
        descriptor=source_table_descriptor)
    return path_source


def create_sink_schema() -> Schema:
    return Schema.new_builder() \
        .column('window_start', DataTypes.TIMESTAMP()) \
        .column('window_end', DataTypes.TIMESTAMP()) \
        .column('pulocationid', DataTypes.INT()) \
        .column('num_hits', DataTypes.BIGINT()) \
        .build()


def create_jdbc_sink(t_env:StreamTableEnvironment, sink_schema:Schema) -> str:
    path_sink = 'jdbc_sink'
    sink_table_descriptor = TableDescriptor.for_connector('jdbc') \
        .schema(sink_schema) \
        .option('url', 'jdbc:postgresql://postgres:5432/postgres') \
        .option('table-name', 'green_aggregated_pulocationid') \
        .option('username', 'postgres') \
        .option('password', 'postgres') \
        .option('driver', 'org.postgresql.Driver') \
        .build()
    t_env.create_temporary_table(
        path=path_sink,
        descriptor=sink_table_descriptor)
    return path_sink


def create_print_sink(t_env:StreamTableEnvironment, sink_schema:Schema) -> str:
    path_sink = 'print_sink'
    sink_table_descriptor = TableDescriptor.for_connector('print') \
        .schema(sink_schema) \
        .build()
    t_env.create_temporary_table(
        path=path_sink,
        descriptor=sink_table_descriptor)
    return path_sink


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(interval=10 * 1000)
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    source_schema = create_source_schema()
    path_source = create_kafka_source(t_env, source_schema)

    sink_schema = create_sink_schema()
    path_sink = create_jdbc_sink(t_env, sink_schema)
    # path_sink = create_print_sink(t_env, sink_schema)

    table = t_env.from_path(path_source)

    try:
        table = table \
            .select(
                col('watermark_strategy'),
                col('pulocationid')) \
            .window(Session.with_gap(lit(5).minutes).on(col('watermark_strategy')).alias('w')) \
            .group_by(col('w'), col('pulocationid')) \
            .select(
                col('w').start.alias('window_start'), 
                col('w').end.alias('window_end'), 
                col('pulocationid'), 
                col('pulocationid').count.alias('num_hits'))

        table.execute_insert(path_sink).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))