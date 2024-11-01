from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
import dags.football_leagues as football_leagues
import pandas as pd
import pytz
import os

# Definición de argumentos por defecto para el DAG
default_args = {
    'owner': 'vcv88',
    'start_date': datetime(2024, 10, 26, tzinfo=pytz.UTC),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Función para ejecutar el script y procesar los datos
def execute_script():
    df_final = football_leagues.data_processing(football_leagues.df_ligas)
    df_final.to_csv('/usr/local/airflow/dags/data/football_positions.csv', index=False)

# Función para leer los archivos .sql
def read_sql_file(filename):
    with open(filename, 'r') as file:
        return file.read()

# Definición del DAG y tareas
with DAG('demo_leagues_dag',
         default_args=default_args,
         schedule_interval='@weekly',
         catchup=False) as dag:

    # Tarea para ejecutar el script y procesar datos
    execute_football_leagues = PythonOperator(
        task_id='execute_football_leagues',
        python_callable=execute_script
    )
    
    upload_stage = SnowflakeOperator(
        task_id='upload_stage',
        sql=read_sql_file('/usr/local/airflow/dags/queries/upload_stage.sql'),
        params={
            'path_file':'/usr/local/airflow/dags/data/football_positions.csv',
            'stage': 'DEMO_STAGE'
        },
        snowflake_conn_id='snow_new',
    )

    # cargar stage en tabña
    upload_table = SnowflakeOperator(
        task_id='upload_table',
        sql='/usr/local/airflow/dags/queries/upload_table.sql',
        params={
            'stage': 'DEMO_STAGE'
        },
        snowflake_conn_id='snow_new'
    )

    execute_football_leagues >> upload_stage >> upload_table
