from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 5),
    'retries': 1,
}

dag = DAG(
    'example_dag', 
    default_args=default_args,
    description='An example DAG',
    schedule_interval='@daily',  # This will run the DAG daily
)

# Define the tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> end_task
