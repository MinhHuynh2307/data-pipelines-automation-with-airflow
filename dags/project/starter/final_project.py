from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements as sqls

default_args = {
    'owner' : 'minhht',
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    start_date=pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    end_date=pendulum.datetime(2018, 11, 3, 0, 0, 0 ,0),
    schedule_interval='@daily',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log-data',
        aws_iam_role='arn:aws:iam::306479194922:role/my-redshift-service-role', 
        region='us-west-2',
        json='s3://udacity-dend/log_json_path.json',
        redshift_conn_id='redshift',
        provide_context=True
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song-data',
        aws_iam_role='arn:aws:iam::306479194922:role/my-redshift-service-role', 
        region='us-west-2',
        json='auto',
        redshift_conn_id='redshift'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        sql_statement=sqls.SqlQueries.songplay_table_insert,
        table='songplays',
        redshift_conn_id='redshift'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_statement=sqls.SqlQueries.user_table_insert,
        table='users',
        append_only=False,
        redshift_conn_id='redshift'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_statement=sqls.SqlQueries.song_table_insert,
        table='songs',
        append_only=False,
        redshift_conn_id='redshift'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_statement=sqls.SqlQueries.artist_table_insert,
        table='artists',
        append_only=False,
        redshift_conn_id='redshift'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_statement=sqls.SqlQueries.time_table_insert,
        table='time',
        append_only=False,
        redshift_conn_id='redshift'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        checks=[{'test_sql' : 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL', 'expected_result' : 0, 'comparison' : '='},
        {'test_sql' : 'SELECT COUNT(*) FROM songs', 'expected_result' : 0, 'comparison' : '>'},
        {'test_sql' : 'SELECT COUNT(*) FROM users', 'expected_result' : 0, 'comparison' : '>'},
        {'test_sql' : 'SELECT COUNT(*) FROM artists', 'expected_result' : 0, 'comparison' : '>'},
        {'test_sql' : 'SELECT COUNT(*) FROM time', 'expected_result' : 0, 'comparison' : '>'}
        ]
    )
 
    end_operator = DummyOperator(task_id = 'End_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_time_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_user_dimension_table >> run_quality_checks >> end_operator
final_project_dag = final_project()