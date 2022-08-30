# import os
# from datetime import datetime
#
# from airflow import DAG, Dataset
# from astro import sql as aql
# from astro.files import File, get_file_list
# from astro.sql.table import Metadata, Table
# from astro.constants import FileType
#
# S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "s3://astromlbucket")
# AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
# SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONN", "snowflake_default")
# SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "PHANIKUMAR")
#
# with DAG(
#     dag_id="load_aggregated_data_to_s3",
#     schedule_interval=None,
#     start_date=datetime(2022, 1, 1),
#     schedule=[
#         Dataset("snowflake://aggregated_data"),
#     ],
#     catchup=False,
# ) as dag:
#     crx_data_table = Table(conn_id=SNOWFLAKE_CONN, name="temp_crx_data_table", metadata=Metadata(
#         schema=SNOWFLAKE_SCHEMA_NAME, ))
#
#     export_data_s3 = aql.export_file(
#         task_id="save_file_to_s3",
#         input_data=crx_data_table,
#         output_file=File(
#             path=f"{S3_BUCKET_NAME}/crx_data.csv",
#             conn_id=AWS_CONN_ID,
#         ),
#         if_exists="replace",
#         outlets=[Dataset("databricks://prediction_input")]
#     )