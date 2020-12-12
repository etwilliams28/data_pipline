from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
        """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id="",
                 table= "",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id,
        self.load_sql_stmt=load_sql_stmt

    def execute(self, context):
        redshift = Postgreshook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_sql.format(self.table,
                                                          self.laod_sql_stmt)
    
        
        self.log.info(f"loading fact table into redshift, table:{self.table}")
        
        redshift.run(formatted_sql)
        
