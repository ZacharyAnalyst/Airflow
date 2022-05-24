from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'zachary',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 24),
    'retries': 0
}

dag = DAG('Pycharm_dag_lect',
          default_args=default_args,
          catchup=False,
          schedule_interval='00 20 * * *')


def create_report_nba():
    import requests
    import pandas as pd

    year = 1999
    download_path_ex = "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv"
    target_path = "nba_all_elo.csv"
    response = requests.get(download_path_ex)
    response.raise_for_status()  # Check the status of the request
    with open(target_path, "wb") as f:
        f.write(response.content)
    print('Successful download!')
    nba = pd.read_csv('nba_all_elo.csv')
    nba = nba.groupby('year_id').game_id.nunique().reset_index()
    nba['rolling_games'] = nba['game_id'].rolling(5).mean()
    # Посчитать разницу между скользащим средним и реальным количеством игр
    nba = int(round(nba[nba['year_id'] == year]['rolling_games'] - nba[nba['year_id'] == year]['game_id']))
    # Описание метрики
    new_report = f"Отклонение в {year} году составляет ровно {nba}"
    # Запись в текстовый файл
    text_file = open('example_report.txt', 'w')
    text_file.write(new_report)
    text_file.close()
    print('Report is written!')


task = PythonOperator(
    task_id='ETL_in_one_step',
    python_callable=create_report_nba,
    dag=dag)
