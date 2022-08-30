import json
import os
from datetime import timedelta
from typing import Dict, Optional

from airflow import DAG, Dataset
from airflow.utils.timezone import datetime
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

DATABRICKS_CONN_ID = os.getenv("ASTRO_DATABRICKS_CONN_ID", "databricks_default")
# Notebook path as a Json object
notebook_task = '{"notebook_path": "/Users/phani.kumar@astronomer.io/Prediction_model_2"}'
NOTEBOOK_TASK = json.loads(os.getenv("DATABRICKS_NOTEBOOK_TASK", notebook_task))
# notebook_params: Optional[Dict[str, str]] = {"Variable": "5"}
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
        dag_id="prediction_analysis_company1",
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_args,
        schedule=[
            Dataset("databricks://prediction_input_company1"),
        ]
) as dag:
    # [START howto_operator_databricks_submit_run_async]
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id="predictive_analytics_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id="0822-084434-jj1oqz3g",
        # new_cluster=new_cluster,
        notebook_task=NOTEBOOK_TASK,
        do_xcom_push=True,
        outlets=[Dataset("s3://prediction_results_company1")]
    )

