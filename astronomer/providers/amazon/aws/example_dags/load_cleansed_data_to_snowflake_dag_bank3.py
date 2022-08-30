import os
from datetime import datetime
from airflow import DAG, Dataset
from astro import sql as aql
from astro.files import File, get_file_list
from astro.sql.table import Metadata, Table
from astro.constants import FileType

S3_BUCKET_NAME_SOURCE = os.getenv("S3_BUCKET_NAME", "s3://ingestionfeeds")
AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
SNOWFLAKE_CONN = os.getenv("SNOWFLAKE_CONN", "snowflake_default")
SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "PHANIKUMAR")

with DAG(
        dag_id="load_cleansed_data_to_snowflake_bank3",
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        schedule=[
            Dataset('s3://ingestionfeeds/crxbank3.csv'),
        ],
        catchup=False,
) as dag:
    crx_data_table = Table(conn_id=SNOWFLAKE_CONN, name="temp_crx_data_bank3", metadata=Metadata(
        schema=SNOWFLAKE_SCHEMA_NAME, ))

    load_table_with_data = aql.load_file(
        input_file=File(path=f"{S3_BUCKET_NAME_SOURCE}/crxbank3.csv",
                        filetype=FileType.CSV, conn_id=AWS_CONN_ID),
        task_id="load_csv_data_bank3",
        output_table=crx_data_table,
        outlets=[Dataset("snowflake://crx_data_bank3")]
    )
