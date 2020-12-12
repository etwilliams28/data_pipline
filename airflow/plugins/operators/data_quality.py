from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id_)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        table = kwargs["params"]["table"]
        redshift_hook = redshift
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]