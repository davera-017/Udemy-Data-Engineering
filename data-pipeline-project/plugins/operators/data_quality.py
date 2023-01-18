from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 dq_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for i, check in enumerate(self.dq_checks):
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            comparison = check.get('comparison')

            records = redshift_hook.get_records(sql)

            if comparison == "=" and (len(records) != exp_result or len(records[0]) != exp_result):
                raise ValueError(f"Data quality check failed. {len(records)} records found but {exp_result} expected.")
            elif comparison == "<" and (len(records) >= exp_result or len(records[0]) >= exp_result):
                raise ValueError(f"Data quality check failed. {len(records)} records found less than {exp_result} expected.")
            elif comparison == ">" and (len(records) <= exp_result or len(records[0]) <= exp_result):
                raise ValueError(f"Data quality check failed. {len(records)} records found more than {exp_result} expected.")
            elif comparison:
                raise ValueError(f"Data quality check failed. No valid comparison operator found. Try <, =, or >.")

            self.log.info(f"{i} check passed. ({check})")

        self.log.info(f"SUCCESS: {self.task_id}")


