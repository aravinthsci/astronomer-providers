import time
from typing import List, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.utils.context import Context
from azure.mgmt.datafactory.models import (
    BlobSource,
    CopyActivity,
    DatasetReference,
    PipelineResource,
    SqlDWSink,
)


class WasbToSynapseOperator(BaseOperator):
    """
    Copies the blob from wasb to synapse.


    :param source_name: place where blob is present
    :param destination_name: destination to where data needs to be copied
    :param translator_type: type of translator
    :param mappings: You can configure the mapping on the Authoring UI -> copy activity -> mapping tab,
        or programmatically specify the mapping in copy activity -> translator property. The following
        properties are supported in translator -> mappings array -> objects -> source and sink,
        which points to the specific column/field to map data.
    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param activity_name: The name of the pipeline to execute.
    :param resource_group_name: The resource group name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the resource group name provided in the corresponding
        connection.
    :param factory_name: The data factory name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the factory name name provided in the corresponding
        connection.
    """

    template_fields: Sequence[str] = (
        "azure_data_factory_conn_id",
        "resource_group_name",
        "factory_name",
        "source_name",
        "destination_name",
        "activity_name",
    )
    template_fields_renderers = {"parameters": "json"}

    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        source_name: str,
        destination_name: str,
        translator_type: str,
        mappings: List[dict],
        azure_data_factory_conn_id: str = AzureDataFactoryHook.default_conn_name,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        activity_name: Optional[str] = "activity",
        check_interval: int = 15,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_name = source_name
        self.destination_name = destination_name
        self.translator_type = translator_type
        self.mappings = mappings
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.activity_name = activity_name
        self.check_interval = check_interval

    def execute(self, context: "Context") -> None:
        """Copies a blob and creates a tablke in synapse using adf pipeline"""
        hook = AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        client = hook.get_conn()
        blob_source = BlobSource()
        dw_sink = SqlDWSink()
        dsin_ref = DatasetReference(reference_name=self.source_name)
        dsOut_ref = DatasetReference(reference_name=self.destination_name)
        copy_activity = CopyActivity(
            name=self.activity_name,
            type="Copy",
            inputs=[dsin_ref],
            outputs=[dsOut_ref],
            source=blob_source,
            sink=dw_sink,
            translator={"type": self.translator_type, "mappings": self.mappings},
        )
        params_for_pipeline = {}
        p_obj = PipelineResource(activities=[copy_activity], parameters=params_for_pipeline)
        client.pipelines.create_or_update(
            self.resource_group_name, self.factory_name, self.activity_name, p_obj
        )
        run_response = client.pipelines.create_run(
            self.resource_group_name, self.factory_name, self.activity_name, parameters={}
        )
        self.run_id = vars(run_response)["run_id"]
        pipeline_run_status = hook.get_pipeline_run_status(
            self.run_id, self.resource_group_name, self.factory_name
        )
        while pipeline_run_status not in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES:
            self.log.info("Sleeping for %s seconds", str(self.check_interval))
            time.sleep(self.check_interval)
            pipeline_run_status = hook.get_pipeline_run_status(
                self.run_id, self.resource_group_name, self.factory_name
            )

        if pipeline_run_status != AzureDataFactoryPipelineRunStatus.SUCCEEDED:
            pipeline_details = hook.get_pipeline_run(self.run_id, self.resource_group_name, self.factory_name)
            self.log.info("path %s", pipeline_details.additional_properties["id"])
            self.log.info("error_message: %s", pipeline_details.message)
            raise AzureDataFactoryPipelineRunException(
                f"Pipeline run {self.run_id} has failed or has been cancelled."
            )
        self.log.info("Pipeline run %s has completed successfully.", self.run_id)
