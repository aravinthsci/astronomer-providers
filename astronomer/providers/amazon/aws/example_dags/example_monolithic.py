import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_SCHEMA = 'PHANIKUMAR'
SNOWFLAKE_STAGE = 'providers_csv_stage'
SNOWFLAKE_WAREHOUSE = 'HUMANS'
SNOWFLAKE_DATABASE = 'SANDBOX'
SNOWFLAKE_ROLE = 'PHANIKUMAR'
SNOWFLAKE_SAMPLE_TABLE = 'crx_bank'
BANK1_DATA_S3_FILE_PATH = 'inputs/crxbank1.csv'
BANK2_DATA_S3_FILE_PATH = 'inputs/crxbank2.csv'
BANK3_DATA_S3_FILE_PATH = 'inputs/crxbank3.csv'
S3_BUCKET_NAME_DESTINATION = os.getenv("S3_BUCKET_NAME_DESTINATION", "ingestionfeeds")
AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
DATABRICKS_CONN_ID = os.getenv("ASTRO_DATABRICKS_CONN_ID", "databricks_default")

notebook_task_2 = '{"notebook_path": "/Users/phani.kumar@astronomer.io/Prediction_model_2"}'
NOTEBOOK_TASK_2 = json.loads(os.getenv("DATABRICKS_NOTEBOOK_TASK", notebook_task_2))

notebook_task_3 = '{"notebook_path": "/Users/phani.kumar@astronomer.io/Prediction_model_3"}'
NOTEBOOK_TASK_3 = json.loads(os.getenv("DATABRICKS_NOTEBOOK_TASK", notebook_task_3))


# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} ( A1 VARCHAR, A2 FLOAT, A3 FLOAT, " \
    f"A4 VARCHAR, A5 VARCHAR, A6 VARCHAR, A7 VARCHAR, A8 FLOAT, A9 VARCHAR, A10 VARCHAR, A11 NUMBER, " \
    f"A12 VARCHAR, A13 VARCHAR, A14 NUMBER, A15 NUMBER, A16 VARCHAR);"
)
default_args = {
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
}


def load_aggregated_data_to_s3_company(**kwargs):
    import time
    sleep_for = kwargs['sleeping_seconds']
    time.sleep(sleep_for)


with DAG(
    dag_id="example_monolithic",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    tags=["example", "async", "snowflake"],
    catchup=False,
) as dag:
    with TaskGroup(group_id='ingestion_monolithic') as tg1:
        ingest_bank1_data = LocalFilesystemToS3Operator(
            task_id="ingest_bank1_data",
            filename="/usr/local/airflow/dags/crxbank1.csv",
            dest_key="inputs/crxbank1.csv",
            dest_bucket=S3_BUCKET_NAME_DESTINATION,
            replace=True,
            aws_conn_id="aws_s3_default",
        )

        ingest_bank2_data = LocalFilesystemToS3Operator(
            task_id="ingest_bank2_data",
            filename="/usr/local/airflow/dags/crxbank2.csv",
            dest_key="inputs/crxbank2.csv",
            dest_bucket=S3_BUCKET_NAME_DESTINATION,
            replace=True,
            aws_conn_id="aws_s3_default",
        )

        ingest_bank3_data = LocalFilesystemToS3Operator(
            task_id="ingest_bank3_data",
            filename="/usr/local/airflow/dags/crxbank3.csv",
            dest_key="inputs/crxbank3.csv",
            dest_bucket=S3_BUCKET_NAME_DESTINATION,
            replace=True,
            aws_conn_id="aws_s3_default",
        )

    create_table = SnowflakeOperator(
        task_id='create_table',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    load_cleansed_data_to_snowflake_bank1 = S3ToSnowflakeOperator(
        task_id='load_cleansed_data_to_snowflake_bank1',
        s3_keys=[BANK1_DATA_S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ',')",
        pattern=".*[.]csv",
    )

    load_cleansed_data_to_snowflake_bank2 = S3ToSnowflakeOperator(
        task_id='load_cleansed_data_to_snowflake_bank2',
        s3_keys=[BANK2_DATA_S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ',')",
        pattern=".*[.]csv",
    )

    load_cleansed_data_to_snowflake_bank3 = S3ToSnowflakeOperator(
        task_id='load_cleansed_data_to_snowflake_bank3',
        s3_keys=[BANK3_DATA_S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ',')",
        pattern=".*[.]csv",
    )

    load_aggregated_data_to_s3_company1 = PythonOperator(
                                            task_id="load_aggregated_data_to_s3_company1",
                                            python_callable=load_aggregated_data_to_s3_company,
                                            op_kwargs={'sleeping_seconds': 3},
                                        )

    load_aggregated_data_to_s3_company2 = PythonOperator(task_id="load_aggregated_data_to_s3_company2",
                                                         python_callable=load_aggregated_data_to_s3_company,
                                                         op_kwargs={'sleeping_seconds': 3},
                                                         )

    load_aggregated_data_to_s3_company3 = PythonOperator(
                                        task_id="load_aggregated_data_to_s3_company3",
                                        python_callable=load_aggregated_data_to_s3_company,
                                        op_kwargs={'sleeping_seconds': 240}
                                        )

    prediction_analysis_company1 = DatabricksSubmitRunOperator(
        task_id="prediction_analysis_company1",
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id="0822-084434-jj1oqz3g",
        notebook_task=NOTEBOOK_TASK_2,
        do_xcom_push=True,
    )

    prediction_analysis_company2 = DatabricksSubmitRunOperator(
        task_id="prediction_analysis_company2",
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id="0822-084434-jj1oqz3g",
        # new_cluster=new_cluster,
        notebook_task=NOTEBOOK_TASK_2,
        do_xcom_push=True,
    )

    prediction_analysis_company3 = DatabricksSubmitRunOperator(
        task_id="prediction_analysis_company3",
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id="0822-084434-jj1oqz3g",
        # new_cluster=new_cluster,
        notebook_task=NOTEBOOK_TASK_3,
        do_xcom_push=True,
    )

    (
        tg1 >>
        create_table
        >> load_cleansed_data_to_snowflake_bank1
        >> load_cleansed_data_to_snowflake_bank2
        >> load_cleansed_data_to_snowflake_bank3
        >> load_aggregated_data_to_s3_company1
        >> load_aggregated_data_to_s3_company2
        >> load_aggregated_data_to_s3_company3
        >> prediction_analysis_company1
        >> prediction_analysis_company2
        >> prediction_analysis_company3
    )