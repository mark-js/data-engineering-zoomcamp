import pendulum
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from airflow.sdk import dag, task, task_group, Param, Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


@task_group
def taxi_pipeline(taxi_type: str):

    @task.short_circuit
    def check_taxi_type(taxi_type: str, **context):
        return taxi_type in context["params"].get("taxi_types", [])

    @task
    def trigger_function(taxi_type: str, ds: str):
        year_month = ds[:7]
        file_name = f"{taxi_type}_tripdata_{year_month}.parquet"
        function_url = Variable.get("gcp_function_url")

        hook = GoogleBaseHook(gcp_conn_id="gcp")
        sa_credentials = hook.get_credentials()
        id_token_credentials = service_account.IDTokenCredentials(
            signer=sa_credentials.signer,
            service_account_email=sa_credentials.service_account_email,
            token_uri="https://oauth2.googleapis.com/token",
            target_audience=function_url,
        )

        session = AuthorizedSession(id_token_credentials)
        response = session.post(
            function_url,
            json={
                "url": f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}",
                "bucket_name": Variable.get("gcp_bucket_name"),
                "object_name": f"taxi_data/{file_name}",
            },
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

    @task.short_circuit
    def check_load_to_bigquery(**context):
        return context["params"].get("load_to_bigquery")

    def bigquery_insert_sql(task_id: str, sql_file: str):
        return BigQueryInsertJobOperator(
            task_id=task_id,
            gcp_conn_id="gcp",
            configuration={
                "query": {
                    "query": f"{{% include '{sql_file}' %}}",
                    "useLegacySql": False,
                }
            }
        )

    create_table = bigquery_insert_sql(task_id="create_table", sql_file=f"sql/{taxi_type}_create_table.sql")
    external_table = bigquery_insert_sql(task_id="external_table", sql_file=f"sql/{taxi_type}_external_table.sql")
    staging_table = bigquery_insert_sql(task_id="staging_table", sql_file=f"sql/{taxi_type}_staging_table.sql")
    merge_table = bigquery_insert_sql(task_id="merge_table", sql_file=f"sql/{taxi_type}_merge_table.sql")

    (
        check_taxi_type(taxi_type=taxi_type)
        >> trigger_function(taxi_type=taxi_type)
        >> check_load_to_bigquery()
        >> create_table
        >> external_table
        >> staging_table
        >> merge_table
    )


@dag(
    schedule="0 0 1 * *",
    start_date=pendulum.datetime(2000, 1, 1, tz="UTC"),
    catchup=False,
    params={
        "taxi_types": Param(
            default=["yellow", "green"],
            type="array",
            title="Taxi types to run",
        ),
        "load_to_bigquery": Param(
            default=True,
            type="boolean",
            title="Load to BigQuery",
        ),
    },
)
def gcp_taxi():
    taxi_pipeline(taxi_type="yellow")
    taxi_pipeline(taxi_type="green")


dag = gcp_taxi()


if __name__ == "__main__":
    dag.test()