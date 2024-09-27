from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Set up logging
logger = logging.getLogger(__name__)

def greet():
    logger.info("Starting the greet task...")
    print("Hello, World!")
    logger.info("Greet task completed successfully.")

def goodbye():
    logger.info("Starting the goodbye task...")
    print("Goodbye, World!")
    logger.info("Goodbye task completed successfully.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=None,
    start_date=datetime(2024, 09, 09),
    catchup=False,
) as dag:

    # Define the tasks
    greet_task = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )

    goodbye_task = PythonOperator(
        task_id='goodbye',
        python_callable=goodbye,
    )

    # Set task dependencies
    greet_task >> goodbye_task
