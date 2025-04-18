#env: python3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime, timedelta


def extract_api_data():
    # Récupération des données de l'API
 response = requests.get('https://api.example.com/rna-data')
    return response.json()

def process_and_load_data(**kwargs):
 ti = kwargs['ti']
 data = ti.xcom_pull(task_ids='extract_data')
    
    # Transformation des données
    # ...
    
    # Chargement dans PostgreSQL
 pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    for item in processed_data:
 pg_hook.run("INSERT INTO rna_data (id, sequence, type, quantity) VALUES (%s, %s, %s, %s)",
 parameters=(item['id'], item['sequence'], item['type'], item['quantity']))

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rna_api_to_postgres',
 default_args=default_args,
 description='Extract RNA data from API and load to PostgreSQL',
 schedule_interval=timedelta(days=1),
)

create_table = PostgresOperator(
 task_id='create_table',
 postgres_conn_id='postgres_conn',
 sql="""
 CREATE TABLE IF NOT EXISTS rna_data (
 id VARCHAR(255) PRIMARY KEY,
 sequence TEXT,
 type VARCHAR(50),
 quantity FLOAT,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 );
 """,
 dag=dag,
)

extract_data = PythonOperator(
 task_id='extract_data',
 python_callable=extract_api_data,
 dag=dag,
)

process_and_load = PythonOperator(
 task_id='process_and_load',
 python_callable=process_and_load_data,
 provide_context=True,
 dag=dag,
)

create_table >> extract_data >> process_and_load