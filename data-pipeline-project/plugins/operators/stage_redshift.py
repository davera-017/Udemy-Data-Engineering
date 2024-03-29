from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info(f"DELETE FROM: {self.table}")
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info(f"COPY FROM S3 TO REDSHIFT")
        self.s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        redshift_hook.run(
            f"""
            COPY {self.table} FROM '{s3_path}'
            REGION 'us-west-2'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS JSON '{self.json_path}'
            """
            )

        self.log.info(f"SUCCESS: {self.task_id}")





