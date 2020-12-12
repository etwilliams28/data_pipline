from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_etl_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    s3_key="udacity-dend",
    s3_bucket="song-data",
    table="staging_songs",
    params="JSON 'auto' COMPUPDATE OFF",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table= "songplay_table",
    load_sql_stmt="songplay_table_insert",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table= "users_table",
    load_sql_stmt="",
    truncate=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table= "songs_table",
    load_sql_stmt="song_table_insert",
    truncate=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table= "artists_table",
    load_sql_stmt="artist_table_insert",
    truncate=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table= "time_table",
    load_sql_stmt="time_table_insert",
    truncate=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    table = "artists_table",
    redshift_conn_id = "redshift",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplay_table
stage_songs_to_redshift >> load_songplay_table
load_songplay_table >> load_user_dimension_table
load_songplay_table >> load_song_dimension_table
load_songplay_table >> load_artist_dimension_table
load_songplay_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
