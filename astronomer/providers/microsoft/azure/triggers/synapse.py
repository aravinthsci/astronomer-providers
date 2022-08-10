import asyncio
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryPipelineRunStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from azure.mgmt.datafactory.models import (
    BlobSource,
    CopyActivity,
    DatasetReference,
    PipelineResource,
    SqlDWSink,
)

from astronomer.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHookAsync,
)


class WasbToSynpaseTrigger(BaseTrigger):
    """
    WasbToSynpaseTrigger is triggered to create a datafactory pipeline , run and check for status
    and exit if it has terminated.

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

    def __init__(
        self,
        azure_data_factory_conn_id: str,
        source_name: str,
        destination_name: str,
        translator_type: str,
        mappings: List[dict[str, Any]],
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        activity_name: Optional[str] = "activity",
        check_interval: int = 15,
    ):
        super().__init__()
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.check_interval = check_interval
        self.activity_name = activity_name
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.source_name = source_name
        self.destination_name = destination_name
        self.translator_type = translator_type
        self.mappings = mappings

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes WasbToSynpaseTrigger arguments and classpath."""
        return (
            "astronomer.providers.microsoft.azure.triggers.synapse.WasbToSynpaseTrigger",
            {
                "azure_data_factory_conn_id": self.azure_data_factory_conn_id,
                "check_interval": self.check_interval,
                "activity_name": self.activity_name,
                "resource_group_name": self.resource_group_name,
                "factory_name": self.factory_name,
                "source_name": self.source_name,
                "destination_name": self.destination_name,
                "translator_type": self.translator_type,
                "mappings": self.mappings,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection to Azure Data Factory, creates,runs and polls for the pipeline run status"""
        try:
            hook = AzureDataFactoryHookAsync(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
            client = await hook.get_async_conn()
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
            p_obj = PipelineResource(activities=[copy_activity], parameters={})
            await client.pipelines.create_or_update(
                self.resource_group_name, self.factory_name, self.activity_name, p_obj
            )
            run_response = await client.pipelines.create_run(
                self.resource_group_name, self.factory_name, self.activity_name, parameters={}
            )
            await client.close()
            self.run_id = vars(run_response)["run_id"]
            pipeline_run_status = await hook.get_adf_pipeline_run_status(
                run_id=self.run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )
            while pipeline_run_status not in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES:
                self.log.info("Sleeping for %s seconds", str(self.check_interval))
                await asyncio.sleep(self.check_interval)
                pipeline_run_status = await hook.get_adf_pipeline_run_status(
                    self.run_id, self.resource_group_name, self.factory_name
                )
            if pipeline_run_status != AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                pipeline_details = await hook.get_pipeline_run(
                    self.run_id, self.resource_group_name, self.factory_name
                )
                self.log.info("path %s", pipeline_details.additional_properties["id"])
                self.log.info("error_message: %s", pipeline_details.message)
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f" Path: https://management.azure.com{pipeline_details.additional_properties['id']} \
                    \n Error Message: The pipeline run {self.run_id} failed due to {pipeline_details.message}.",
                        "run_id": self.run_id,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "Success",
                        "message": f"The pipeline run {self.run_id} has {pipeline_run_status}.",
                        "run_id": self.run_id,
                    }
                )
        except Exception as e:
            await client.close()
            yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})
