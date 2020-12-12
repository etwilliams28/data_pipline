from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """
    
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 aws_conn_id ="",
                 table = "",
                 load_sql_stmt = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id =redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        sel.turncate = truncate

    def execute(self, context):
        redshift = PostresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'load dimentions table {self.table} to redshift')
        
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.load.sql.stmt
        )
        
        redshift_run(formatted_sql)
