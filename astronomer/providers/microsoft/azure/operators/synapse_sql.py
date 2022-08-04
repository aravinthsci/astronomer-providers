from airflow.models import BaseOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.context import Context


class SynapseSQLOperator(BaseOperator):
    """
    Run a query on synapse database with a dedicated pool

    :param sql: query to be executed on synapse analytics
    """

    def __init__(
        self,
        *,
        sql: str,
        synapse_sql_conn_id: str = OdbcHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.synapse_sql_conn_id = synapse_sql_conn_id

    def execute(self, context: "Context") -> None:
        """Using ODBC connection run the sql query on the given server"""
        hook = OdbcHook()
        results = hook.get_records(self.sql)
        print(results)
