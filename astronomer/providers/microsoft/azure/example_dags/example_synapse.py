import os
from datetime import datetime, timedelta

from airflow import DAG

from astronomer.providers.microsoft.azure.operators.synapse import WasbToSynapseOperator
from astronomer.providers.microsoft.azure.operators.synapse_sql import (
    SynapseSQLOperator,
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "azure_data_factory_conn_id": "azure_data_factory_default",
}

with DAG(
    dag_id="example_synapse",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "Synapse", "Azure"],
) as dag:
    wasb_to_synapse = WasbToSynapseOperator(
        task_id="wasb_to_synapse",
        source_name="DelimitedText2",
        destination_name="AzureSynapseAnalyticsTable2",
        resource_group_name="team_provider_resource_group_test",
        factory_name="providersdf",
        activity_name="copy_activity_rajath",
        translator_type="TabularTranslator",
        mappings=[
            {"source": {"name": "column1"}, "sink": {"name": "col1"}},
            {"source": {"name": "column2"}, "sink": {"name": "col2"}},
        ],
    )
    synapse_sql_query = SynapseSQLOperator(
        task_id="synapse_sql_query",
        sql="SELECT TOP (10) [col1],[col2] from [dbo].[table_with_multiple_cols]",
    )
