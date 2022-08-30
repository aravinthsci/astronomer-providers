# import os
# from datetime import datetime
#
# from airflow import DAG
#
# from astro import sql as aql
# from astro.sql import run_raw_sql
# from astro.sql.operators.load_file import LoadFileOperator as LoadFile
# from astro.files import File, get_file_list
# from astro.sql.table import Metadata, Table
#
# S3_BUCKET_NAME_SOURCE = os.getenv("S3_BUCKET_NAME", "s3://test-crx-data-with-header/")
# S3_BUCKET_NAME_DESTINATION = os.getenv("S3_BUCKET_KEY_SOURCE", "s3://astromlbucket")
# # S3_BUCKET_KEY = os.getenv("S3_BUCKET_KEY", "test")
# AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
# SQLITE_CONN = os.getenv("SQLITE_CONN", "sqlite_conn")
# SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONN", "snowflake_default")
# SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "PHANIKUMAR")
#
# @aql.transform()
# def order_by_index_column(input_table: Table):
#     return """
#         SELECT "A1","A2","A3","A4","A5","A6","A7","A8","A9","A10","A11","A12","A13","A14","A15","A16"
#         FROM {{input_table}}
#     """
#
# @run_raw_sql
# def create_table(table: Table):
#     """Create the temp_crx_data table which will be the target of the append method"""
#     return """
#     CREATE OR REPLACE TRANSIENT TABLE {{table}} (
#       "A1" varchar,
#         "A2" float,
#         "A3" varchar,
#         "A4" varchar,
#         "A5" varchar,
#         "A6" varchar,
#         "A7" varchar,
#         "A8" float,
#         "A9" varchar,
#         "A10" varchar,
#         "A11" varchar,
#         "A12" varchar,
#         "A13" varchar,
#         "A14" int,
#         "A15" varchar,
#         "A16" varchar
#     );
#     """
#
#
# with DAG(
#     dag_id="example_dynamic_task_template",
#     schedule_interval=None,
#     start_date=datetime(2022, 1, 1),
#     catchup=False,
# ) as dag:
#
#     credit_card_table = Table(conn_id=SNOWFLAKE_CONN, name="temp_crx_data",  metadata=Metadata(
#         schema=SNOWFLAKE_SCHEMA_NAME,))
#     create_results_table = create_table(
#         table=credit_card_table, conn_id=SNOWFLAKE_CONN
#     )
#     #
#     # load_table_with_data = LoadFile.partial(
#     #     task_id="load_file_s3_to_tables",
#     #     output_table=credit_card_table,
#     #     use_native_support=False,
#     #     if_exists="append",
#     #     columns_names_capitalization="upper"
#     # ).expand(input_file=get_file_list(path=S3_BUCKET_NAME_SOURCE, conn_id=AWS_CONN_ID))
#
#
#     ordered_table_output = order_by_index_column(
#         input_table=credit_card_table,
#         output_table=Table(conn_id=SNOWFLAKE_CONN, name="crx_data", metadata=Metadata(
#                 schema=SNOWFLAKE_SCHEMA_NAME,
#             )),
#     )
#
#     export_data_s3 = aql.export_file(
#         task_id="save_file_to_s3",
#         input_data=ordered_table_output,
#         output_file=File(
#             path=f"{S3_BUCKET_NAME_DESTINATION}/transformed_crx_data/crx_data.csv",
#             conn_id=AWS_CONN_ID,
#         ),
#         if_exists="replace",
#     )
#
#     create_results_table >> load_table_with_data >> ordered_table_output >> export_data_s3