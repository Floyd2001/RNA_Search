from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'rna_project',
    'start_date': datetime(2025, 4, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'rna_target_mapping',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
)

def fetch_ttd_targets():
    # Exemple : téléchargement ou parsing d’un fichier TTD (ou depuis leur site)
    response = requests.get("/api/v1/rna/{}/protein-targets/{}/")
    if response.status_code == 200:
        return response.json()
    return []

if __name__ == "__main__":
    test = fetch_ttd_targets()
    print(test)

    
def enrich_with_rnacentral(**kwargs):
    ti = kwargs['ti']
    ttd_data = ti.xcom_pull(task_ids='fetch_ttd')

    enriched_data = []
    for target in ttd_data:
        target_name = target.get('name')
        response = requests.get(f"https://rnacentral.org/api/v1/rna/?target={target_name}")
        if response.status_code == 200:
            for rna in response.json().get('results', []):
                enriched_data.append({
                    'rna_id': rna['rnacentral_id'],
                    'rna_name': rna.get('short_name', ''),
                    'rna_type': rna.get('rna_type', ''),
                    'target_name': target_name,
                    'target_id': target.get('id', ''),
                    'source': 'RNAcentral',
                    'confidence_level': 'experimental'  # ou à ajuster
                })
    return enriched_data

def insert_into_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='enrich_rna')

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    for row in data:
        pg_hook.run("""
            INSERT INTO rna_targets (rna_id, rna_name, rna_type, target_name, target_id, source, confidence_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (rna_id, target_id) DO NOTHING;
        """, parameters=(row['rna_id'], row['rna_name'], row['rna_type'],
                         row['target_name'], row['target_id'], row['source'], row['confidence_level']))

fetch_ttd = PythonOperator(
    task_id='fetch_ttd',
    python_callable=fetch_ttd_targets,
    dag=dag,
)

enrich_rna = PythonOperator(
    task_id='enrich_rna',
    python_callable=enrich_with_rnacentral,
    provide_context=True,
    dag=dag,
)

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=insert_into_db,
    provide_context=True,
    dag=dag,
)

fetch_ttd >> enrich_rna >> insert_data
