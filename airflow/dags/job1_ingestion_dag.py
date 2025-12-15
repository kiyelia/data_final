from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

with DAG(
    dag_id='job1_api_ingestion_to_kafka',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    catchup=False,
    tags=['project', 'kafka', 'ingestion'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
    }
) as dag:
    
    # Executes the Python producer script using the path inside the container.
    run_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python /opt/airflow/src/job1_producer.py',
    )
