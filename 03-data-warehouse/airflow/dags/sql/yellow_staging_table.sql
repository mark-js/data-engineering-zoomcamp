CREATE OR REPLACE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.gcp_dataset }}.yellow_tripdata_staging` AS
SELECT
    MD5(
        CONCAT(
            COALESCE(CAST(VendorID AS STRING), ""),
            COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
            COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
            COALESCE(CAST(PULocationID AS STRING), ""),
            COALESCE(CAST(DOLocationID AS STRING), "")
        )
    ) AS unique_row_id,
    'yellow_tripdata_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }}.parquet' AS filename,
    *
FROM `{{ var.value.gcp_project_id }}.{{ var.value.gcp_dataset }}.yellow_tripdata_external`;