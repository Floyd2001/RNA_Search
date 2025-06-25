from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'rna_project',
    'start_date': datetime(2025, 6, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}
dag = DAG(
        'rna_target_mapping',
        default_args=default_args,
        schedule="@weekly", # Sunday at 00:00
        catchup=False,
    )

def fetch_rna_from_rnacentral():
    url = "https://rnacentral.org/api/v1/rna/?page_size=100"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get('results', [])
    else:
        print(f"Error fetching data from RNAcentral: {response.status_code}")
        return []

if __name__ == "__main__":
    test = fetch_rna_from_rnacentral()
    print(test)


create_table = PostgresOperator(
    task_id='create_rna_targets_table',
    postgres_conn_id='postgres_conn',
    sql="airflow-docker/init-scripts/01-create-tables.sql",
    dag=dag,
)


def insert_into_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='data_rna')

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    for row in data:
        pg_hook.run("""
            INSERT INTO rna_targets (rna_id, rna_name, rna_type, target_name, target_id, source, confidence_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (rna_id, target_id) DO NOTHING;
        """, parameters=(row['rna_id'], row['rna_name'], row['rna_type'],
                         row['target_name'], row['target_id'], row['source'], row['confidence_level']))


fetch_rna = PythonOperator(
    task_id='fetch_rna',
    python_callable=fetch_rna_from_rnacentral,
    dag=dag,
)



insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=insert_into_db,
    dag=dag,
)

fetch_rna >> insert_data
