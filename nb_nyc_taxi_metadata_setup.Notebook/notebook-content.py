# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4e2ecbec-4240-484a-8911-17a9f79e6e04",
# META       "default_lakehouse_name": "lh_nyc_taxi",
# META       "default_lakehouse_workspace_id": "9d45f741-cd26-4f6e-98e7-bc144549bf28",
# META       "known_lakehouses": [
# META         {
# META           "id": "4e2ecbec-4240-484a-8911-17a9f79e6e04"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql('''CREATE OR REPLACE TABLE metadata_and_confg_details.metadata_dataset_config (
    dataset_id    STRING    NOT NULL,
    dataset_name  STRING    NOT NULL,
    file_prefix   STRING    NOT NULL,
    source_url    STRING,
    file_format   STRING    NOT NULL,
    is_active     BOOLEAN   NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL
) USING DELTA;
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql('''INSERT INTO metadata_and_confg_details.metadata_dataset_config VALUES
('DS001', 'Yellow', 'yellow_tripdata', 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page', 'parquet', true, current_timestamp(), current_timestamp()),
('DS002', 'Green',  'green_tripdata',  'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page', 'parquet', true, current_timestamp(), current_timestamp()),
('DS003', 'FHV',    'fhv_tripdata',    'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page', 'parquet', true, current_timestamp(), current_timestamp()),
('DS004', 'HVFHV',  'fhvhv_tripdata',  'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page', 'parquet', true, current_timestamp(), current_timestamp());''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql('SELECT * FROM metadata_and_confg_details.metadata_dataset_config;'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# New control table: one row per dataset / year / month, with ETag and size for change detection
spark.sql('''
CREATE TABLE IF NOT EXISTS metadata_and_confg_details.file_control (
    unique_run_id            STRING,
    file_id                  STRING    NOT NULL,
    dataset_id               STRING    NOT NULL,
    file_name                STRING    NOT NULL,
    download_url             STRING    NOT NULL,
    landing_path             STRING,
    ingestion_status         STRING    NOT NULL,
    transformation_status    STRING    NOT NULL,
    current_etag             STRING,
    previous_etag            STRING,
    current_content_length   BIGINT,
    previous_content_length  BIGINT,
    data_year                INT       NOT NULL,
    data_month               INT       NOT NULL,
    record_count             BIGINT,
    total_retries            INT       NOT NULL,
    error_message            STRING,
    ingested_at              TIMESTAMP,
    created_at               TIMESTAMP NOT NULL,
    updated_at               TIMESTAMP NOT NULL
) USING DELTA
''')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
