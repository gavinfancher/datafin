from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import your datafin package
import datafin

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def test_datafin_function():
    """
    Test function that uses the datafin package.
    Replace this with actual functionality from your package.
    """
    print("Testing datafin package integration")
    # Add your package usage here
    return "Success!"

with DAG(
    'test_datafin_dag',
    default_args=default_args,
    description='A test DAG for datafin package',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='test_datafin_task',
        python_callable=test_datafin_function,
    ) 