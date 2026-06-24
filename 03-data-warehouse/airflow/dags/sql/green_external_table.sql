CREATE OR REPLACE EXTERNAL TABLE `{{ var.value.gcp_project_id }}.{{ var.value.gcp_dataset }}.green_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{{ var.value.gcp_bucket_name }}/taxi_data/green_tripdata_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }}.parquet']
);
