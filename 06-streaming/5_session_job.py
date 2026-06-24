from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


def create_events_aggregated_sink(t_env, table_name, partition_by):
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            {partition_by} INTEGER,
            num_hits BIGINT,
            PRIMARY KEY (window_start, {partition_by}) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env, topic):
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            pulocationid INTEGER,
            dolocationid INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_watermark AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' MINUTES
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = '{topic}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation(topic, table_name, partition_by):
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(interval=10 * 1000)
    # env.get_checkpoint_config().enable_unaligned_checkpoints()
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_minutes(1))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env, topic)
        aggregated_table = create_events_aggregated_sink(t_env, table_name, partition_by)

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start,
            window_end,
            {partition_by},
            COUNT(*) AS num_hits
        FROM TABLE(
            SESSION(TABLE {source_table} PARTITION BY {partition_by}, DESCRIPTOR(event_watermark), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, {partition_by};
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation('green-trips', 'green_aggregated_pulocationid', 'pulocationid')