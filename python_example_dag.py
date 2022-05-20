from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime   

default_args = {
  'owner' = 'Svirin Ruslan',
  'depends of past' = False, 
  'start date' = datetime(2022, 5, 20),
  'retries' = 0,
}

dag_2 = DAG('calc_example',
      owner = default_args, 
      catchup = False,
      schedule_interval='00 20 * * *')

def some_function():
  return print('some action') 

def bash_function():
  return echo 'That's correct'

tt1 = PythonOperator(
  task_id = 'realisation of the Py function',
  python_callable = some_function,
  dag=dag_2
)

tt2 = BashOperator(
  task_id = 'realisation of the Bash function',
  bash_command = bash_function,
  dag = dag_2
)

tt1 >> tt2 

