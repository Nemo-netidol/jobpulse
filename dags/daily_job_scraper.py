from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.scraper.remoteOK import RemoteOKScraper
from include.adapters.remoteOK_adapter import RemoteOKAdapter
import pendulum
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json

from include.database import Database
from include.embedding_service import EmbeddingService
from include.vector_db import VectorDatabase

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


default_args = {
    'owner': 'jobpulse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_job_scraper',
    default_args=default_args,
    description='Scrape jobs from RemoteOK daily',
    schedule='0 2 * * *',
    start_date=pendulum.today('UTC').add(days=-2),
    catchup=True
)

# Task: Scrape remoteOK
def scrape_remoteOK():
    """Fetch and store jobs from remoteOK"""
    print("Starting RemoteOK scrape...")
    scraper = RemoteOKScraper()
    raw_jobs = scraper.scrape_jobs()

    print(f"Fetched {len(raw_jobs)} raw jobs from RemoteOK")

    db = Database()
    success_count = 0

    for job in raw_jobs:
        try:
            standard_job = RemoteOKAdapter.transform(job)
            if db.insert_job(standard_job):
                success_count += 1
        except Exception as e:
            print(f"Error processing job:", e)
            continue

    print(f"Successfully stored {success_count}/{len(raw_jobs)} RemoteOK jobs")

    # Export to jobs.json for Streamlit seeding
    try:
        root_json_path = os.path.join(project_root, 'jobs.json')
        
        existing_jobs = []
        if os.path.exists(root_json_path):
            with open(root_json_path, 'r') as f:
                try:
                    existing_jobs = json.load(f)
                except json.JSONDecodeError:
                    existing_jobs = []

        existing_ids = {j.get('id') for j in existing_jobs}
        # Filter out duplicate jobs
        new_jobs = [j for j in raw_jobs if j.get('id') not in existing_ids]

        combined_jobs = existing_jobs + new_jobs

        with open(root_json_path, 'w') as f:
            json.dump(combined_jobs, f)
        print(f"Exported {len(new_jobs)} news jobs to {root_json_path}. Total jobs: {len(combined_jobs)}")
    except Exception as e:
        print(f"Optional JSON export failed: {e}")

    return success_count

def sync_embedding():
    db = Database()
    vector_db = VectorDatabase()
    service = EmbeddingService(db, vector_db)

    result = service.sync_embeddings()

    return result

def summary():
    sql_db = Database()
    embed_result = sql_db.get_stats()
    summary = f"""
    ====================================
    JobPulse Daily Run Summary
    ====================================
    Database Status:
       Total Jobs: {embed_result.get('total_jobs', 0)}
       With Embeddings: {embed_result.get('embedded_jobs', 0)}
       Pending: {embed_result.get('pending_embeddings', 0)}
    """
    print(summary)
    return summary

task_remoteOK = PythonOperator(
    task_id='scrape_remoteOK',
    python_callable=scrape_remoteOK,
    dag=dag
)

task_sync = PythonOperator(
    task_id='sync_embeddings',
    python_callable=sync_embedding,
    dag=dag
)

task_summary = PythonOperator(
    task_id='summary',
    python_callable=summary,
    dag=dag
)

task_remoteOK >> task_sync >> task_summary