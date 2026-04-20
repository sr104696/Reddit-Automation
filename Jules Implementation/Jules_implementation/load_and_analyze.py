"""
Reddit Pipeline Orchestrator DAG

This DAG orchestrates the data flow between the microservices:
1. Verifies the Extractor API is responsive (HttpSensor).
2. Triggers the AI Scoring Batch API script (BashOperator).
3. Triggers the RAG backend vector embedding update (SimpleHttpOperator).
"""

import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    'owner': 'Astro',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='reddit_pipeline_orchestrator',
    default_args=default_args,
    description='Orchestrates the Reddit Intelligence Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'pipeline', 'ai', 'rag'],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    max_active_tasks=1,
)
def reddit_pipeline():
    
    # Task 1: Verify the Extractor API is responsive
    verify_extractor = HttpSensor(
        task_id='verify_extractor',
        http_conn_id='extractor_api_conn', # Requires configuration in Airflow UI or connections
        endpoint='/health',
        method='GET',
        response_check=lambda response: response.json().get('status') == 'healthy',
        poke_interval=60,
        timeout=600,
    )
    
    # Task 2: Trigger AI Batch API (BashOperator running python script from Stage 4)
    # The ai_batch_worker needs to have access to this, so we use docker exec or we assume this runs in an environment with the code
    # Using Docker exec from airflow is complex. Alternatively, run the script directly if the code is mounted.
    # Assuming code is mounted or accessible:
    run_ai_scoring = BashOperator(
        task_id='run_ai_scoring',
        bash_command='cd /app && python -m gpt.batch_api', # Adjust command as necessary for Stage 4 execution
    )
    
    # Task 3: Trigger RAG vector embeddings update
    trigger_rag_update = SimpleHttpOperator(
        task_id='trigger_rag_update',
        http_conn_id='rag_backend_conn', # Requires configuration in Airflow UI
        endpoint='/rag/refresh',
        method='POST',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
    )
    
    verify_extractor >> run_ai_scoring >> trigger_rag_update

# Create the DAG instance
reddit_pipeline_dag = reddit_pipeline()
