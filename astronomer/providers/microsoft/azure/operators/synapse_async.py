from typing import Any, Dict, List, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from airflow.utils.context import Context

from astronomer.providers.microsoft.azure.triggers.synapse import WasbToSynpaseTrigger


class WasbToSynapseOperatorAsync(BaseOperator):
    """
    Copies the blob from wasb to synapse asynchronously.

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
        mappings: List[dict[str, Any]],
        azure_data_factory_conn_id: str = AzureDataFactoryHook.default_conn_name,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        activity_name: Optional[str] = "activity",
        check_interval: int = 15,
        **kwargs: Any,
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
        self.defer(
            timeout=self.execution_timeout,
            trigger=WasbToSynpaseTrigger(
                azure_data_factory_conn_id=self.azure_data_factory_conn_id,
                activity_name=self.activity_name,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
                check_interval=self.check_interval,
                source_name=self.source_name,
                destination_name=self.destination_name,
                translator_type=self.translator_type,
                mappings=self.mappings,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[Any, Any], event: Dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
