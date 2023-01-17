from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 query,
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

        def execute(self, context):
            redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
            if self.truncate:
                redshift_hook.run(f"TRUNCATE TABLE {self.table}")
            redshift_hook.run(f"INSERT INTO {self.table} {self.query}")
            self.log.info(f"SUCCESS: {self.task_id}")
