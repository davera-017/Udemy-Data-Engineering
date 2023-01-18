from datetime import datetime, timedelta
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 12, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('final_project_legacy',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    table="users",
    query=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    table="songs",
    query=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    table="artists",
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    table="time",
    query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0, 'comparison': "="},
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0, 'comparison': "="},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0, 'comparison': "="},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0, 'comparison': "="},
        {'check_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0, 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0, 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0, 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0, 'comparison': ">"},
    ],
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator >> [
                    stage_events_to_redshift,
                    stage_songs_to_redshift
                   ]
[
    stage_events_to_redshift,
    stage_songs_to_redshift
] >> load_songplays_table
load_songplays_table >> [
                            load_artist_dimension_table,
                            load_song_dimension_table,
                            load_time_dimension_table,
                            load_user_dimension_table
                         ]
[
    load_artist_dimension_table,
    load_song_dimension_table,
    load_time_dimension_table,
    load_user_dimension_table
 ] >> run_quality_checks
run_quality_checks >> end_operator
