from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
	'owner': 'zachary',
	'depends_on_past': False,
	'start_date': datetime(2022, 5, 22),
	'retries': 0
}

dag = DAG('calc_example',
      default_args=default_args,
      catchup=False,
      schedule_interval='00 20  * * *')

def some_function():
    return print('some action')

t1 = PythonOperator(
  task_id='py_func',
  python_callable=some_function,
  dag=dag
)

t2 = BashOperator(
  task_id='bash_func',
  bash_command='echo "Hello, I am a value!"',
  dag=dag
)

t1 >> t2
