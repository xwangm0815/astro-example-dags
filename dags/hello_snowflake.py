from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging
from airflow.models import Connection




# Set up logging
logger = logging.getLogger(__name__)
SNOWFLAKE_CONN_ID='xwsf_conn'


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
    "snowflake_conn_id":"xwsf_conn",
   'retries': 2, # Set number of retries
   'retry_delay': timedelta(minutes=1),
}


# Create the DAG
with DAG(
   dag_id='hello_snowflake',
   default_args=default_args,
   description='Snowflake DAG',
   start_date=datetime(2024, 9, 26),
   schedule_interval=None,
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


   run_query = SnowflakeOperator(
       task_id='run_snowflake_query',
       snowflake_conn_id='xwsf_conn',
       sql='SELECT COUNT(*) FROM test.test.test;'
   )


   # Set task dependencies
   greet_task >> run_query >> goodbye_task
