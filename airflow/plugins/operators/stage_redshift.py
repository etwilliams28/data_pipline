from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    load_sql="""
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID "{}"
    SECERET_ACCESS_KEY "{}"
    REGION AS {} {}
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id="",
                 table="",
                 s3_key="",
                 s3_bucket="",
                 params="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
    
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.aws_conn_id=aws_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.params = params
        self.region = region

       

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Copying data from S3 {self.table} table and sending to redshift")
        render_key = self.s3_key.format(**context)
        self.log.info(f"Redner Key: {rednered_key}")
        s3_path = f"s3://{self.s3_bucket}/{render_key}"
        
        formatted_sql = StageToRedshiftOperator.load_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.params
        )
        self.log.info(f"executing quering")
        redshift.run(formatted_sql)
        





