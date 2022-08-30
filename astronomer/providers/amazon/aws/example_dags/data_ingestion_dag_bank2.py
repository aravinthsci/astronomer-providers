# import os
# from datetime import datetime
#
# from airflow import DAG, Dataset
#
# from astro import sql as aql
# from astro.sql import run_raw_sql
# from astro.sql.operators.load_file import LoadFileOperator as LoadFile
# from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
# from astro.files import File, get_file_list
# from astro.sql.table import Metadata, Table
# from airflow.utils.task_group import TaskGroup
#
# S3_BUCKET_NAME_SOURCE = os.getenv("S3_BUCKET_NAME", "s3://test-crx-data-with-header/")
# S3_BUCKET_NAME_DESTINATION = os.getenv("S3_BUCKET_KEY_SOURCE", "s3://astromlbucket")
# # S3_BUCKET_KEY = os.getenv("S3_BUCKET_KEY", "test")
# AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
# SQLITE_CONN = os.getenv("SQLITE_CONN", "sqlite_conn")
# SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONN", "snowflake_default")
# SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "PHANIKUMAR")
#
# dataset2 = Dataset("s3://samplebank2/crxbank2.csv")
#
# with DAG(
#         dag_id="data_ingestion_bank2",
#         schedule_interval=None,
#         start_date=datetime(2022, 1, 1),
#         catchup=False,
# ) as dag:
#
#     ingest_bank2_data = LocalFilesystemToS3Operator(
#         task_id="ingest_bank2_data",
#         filename="/usr/local/airflow/dags/crxbank2.csv",
#         dest_key="crxbank2.csv",
#         dest_bucket="samplebank2",
#         replace=True,
#         aws_conn_id="aws_s3_default",
#         outlets=[dataset2]
#     )