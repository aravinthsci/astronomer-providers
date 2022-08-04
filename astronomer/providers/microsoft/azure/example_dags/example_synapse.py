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
        source_name="DelimitedText3",
        destination_name="AzureSynapseAnalyticsTable3",
        resource_group_name="team_provider_resource_group_test",
        factory_name="providersdf",
        activity_name="copy_active_business_locations",
        translator_type="TabularTranslator",
        mappings=[
            {"source": {"name": "Location Id"}, "sink": {"name": "col1"}},
            {"source": {"name": "Business Account Number"}, "sink": {"name": "col2"}},
            {"source": {"name": "Ownership Name"}, "sink": {"name": "col3"}},
            {"source": {"name": "DBA Name"}, "sink": {"name": "col4"}},
            {"source": {"name": "Street Address"}, "sink": {"name": "col5"}},
            {"source": {"name": "City"}, "sink": {"name": "col6"}},
            {"source": {"name": "State"}, "sink": {"name": "col7"}},
            {"source": {"name": "Source Zipcode"}, "sink": {"name": "col8"}},
            {"source": {"name": "Business Start Date"}, "sink": {"name": "col9"}},
            {"source": {"name": "Business End Date"}, "sink": {"name": "col10"}},
            {"source": {"name": "Location Start Date"}, "sink": {"name": "col11"}},
            {"source": {"name": "Location End Date"}, "sink": {"name": "col12"}},
            {"source": {"name": "Mail Address"}, "sink": {"name": "col13"}},
        ],
    )
    synapse_sql_query = SynapseSQLOperator(
        task_id="synapse_sql_query",
        sql="SELECT TOP (10) [col1],[col2] from [dbo].[active_business_locations]",
    )

    wasb_to_synapse >> synapse_sql_query
