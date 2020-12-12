from airflow.hook.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = ""
    
    insert_sql = """
    INSERT INTO {}
    {}
    ;
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id=="",
                 table="",
                 load_sql_stmt="",
                 *args, **kwargs):
        
        super(LoadFactOperator,
