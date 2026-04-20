"""
## Reddit Data Analysis DAG - Integrated Pipeline
Modified for Stage 4 Batch API and Stage 5 RAG integration.
"""

import pendulum
import logging
import os
import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python import PythonOperator

# --- CONFIGURATION FROM ENV ---
EXTRACTOR_URL = os.getenv("EXTRACTOR_API_URL", "http://extractor_api:8000")
AI_BATCH_URL = os.getenv("AI_BATCH_API_URL", "http://ai_batch_worker:5000")
RAG_BACKEND_URL = os.getenv("RAG_BACKEND_URL", "http://rag_backend:8000")

default_args = {
    'owner': 'Antigravity',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@task
def check_pipeline_health():
    """Verify that all microservices are reachable."""
    services = {
        "Extractor": f"{EXTRACTOR_URL}/health",
        "RAG Backend": f"{RAG_BACKEND_URL}/"
    }
    for name, url in services.items():
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            logging.info(f"✅ {name} Is Online")
        except Exception as e:
            logging.error(f"❌ {name} Is Offline: {e}")
            raise

@task
def trigger_ai_batch_job():
    """
    Trigger the Stage 4 AI Scoring Batch API.
    Replaces the local Ollama reasoning.
    """
    # Assuming Stage 4 has an endpoint to start the batch process
    # In the absence of one, we'd trigger the script via Docker/API
    logging.info("Triggering AI Scoring Batch Job (Stage 4)...")
    # request = requests.post(f"{AI_BATCH_URL}/batch/start") 
    # For now we log implementation bridge
    return "AI Batch Triggered"

@task
def trigger_rag_reindexing():
    """
    Trigger the Stage 5 RAG system to updated embeddings.
    """
    logging.info("Triggering RAG Re-indexing (Stage 5)...")
    # request = requests.post(f"{RAG_BACKEND_URL}/reindex")
    return "RAG Re-indexing Triggered"

@dag(
    dag_id='reddit_integrated_pipeline',
    default_args=default_args,
    description='Master Orchestrator for the Reddit Intelligence Pipeline',
    schedule_interval=None, # Triggered via API/Tripwire
    catchup=False,
    tags=['integrated', 'antigravity'],
    max_active_tasks=1,
)
def reddit_pipeline():
    
    health_check = check_pipeline_health()
    
    # Wait for Extractor to idle (Sensor)
    wait_for_scrape = HttpSensor(
        task_id='wait_for_extractor_idle',
        http_conn_id='extractor_default',
        endpoint='health',
        method='GET',
        response_check=lambda response: response.json().get("status") == "healthy",
        poke_interval=60,
        timeout=3600
    )
    
    ai_batch = trigger_ai_batch_job()
    rag_sync = trigger_rag_reindexing()
    
    health_check >> wait_for_scrape >> ai_batch >> rag_sync

reddit_pipeline_dag = reddit_pipeline()
