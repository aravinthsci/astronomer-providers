# import os
# from datetime import datetime
# from airflow import DAG, Dataset
# from astro import sql as aql
# from astro.files import File, get_file_list
# from astro.sql.table import Metadata, Table
# from astro.constants import FileType
#
# S3_BUCKET_NAME_SOURCE = os.getenv("S3_BUCKET_NAME", "s3://test-bharani")
# AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
# SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONN", "snowflake_default")
# SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "PHANIKUMAR")
#
# with DAG(
#     dag_id="load_cleansed_data_to_snowflake",
#     schedule_interval=None,
#     start_date=datetime(2022, 1, 1),
#     schedule=[
#         Dataset('s3://samplebank1/crxbank1.csv'),
#         Dataset('s3://samplebank2/crxbank2.csv'),
#     ],
#     catchup=False,
# ) as dag:
#
#     crx_data_table = Table(conn_id=SNOWFLAKE_CONN, name="temp_crx_data_table",  metadata=Metadata(
#         schema=SNOWFLAKE_SCHEMA_NAME,))
#
#     load_table_with_data = aql.load_file(
#         input_file=File(path=f"{S3_BUCKET_NAME_SOURCE}/crx_dataset/",
#                         filetype=FileType.CSV, conn_id=AWS_CONN_ID),
#         task_id="load_csv_data",
#         output_table=crx_data_table,
#         outlets=[Dataset("snowflake://aggregated_data")]
#     )