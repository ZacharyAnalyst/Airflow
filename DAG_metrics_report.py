from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'zachary',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 30),
    'retries': 0,
}

dag = DAG('dag_metrics_report',
          default_args=default_args,
          catchup=False,
          schedule_interval='00 12 * * 1',
          )


def report_to_vk():
    import pandas as pd
    import vk_api
    import random

    path = "/home/zachary/Personal/Data_analysis/data_analys_by_Anatoly_Karpov/[SW.BAND] 4 AirFlow/[SW.BAND] Задания/ads_data_121288 - ads_data_121288.csv"
    ads_df = pd.read_csv(path, parse_dates=[0])

    # Расчет метрик в разрезе каждого дня: количество показов,
    # количество кликов, CTR, сумма потраченных денег.
    ads_click = ads_df.query("event == 'click'") \
        .groupby(by=['date']) \
        .agg(number_of_clicks=('event', 'count')).reset_index()

    ads_view = ads_df.query("event == 'view'") \
        .groupby(by=['date']) \
        .agg(number_of_view=('event', 'count')).reset_index()

    ads_df_merged = pd.merge(ads_click, ads_view, on='date')
    ads_df_merged['ctr'] = round(ads_df_merged['number_of_clicks'] / ads_df_merged['number_of_view'], 4)
    ads_df_merged['ad_expenses'] = ads_df.ad_cost[0] / 1000 * ads_df_merged['number_of_view']

    # Найдем отклонение каждой метрики с шагом в 1 день
    diff_values = ((ads_df_merged.iloc[1, 1:5] - ads_df_merged.iloc[0, 1:5]) / ads_df_merged.iloc[0, 1:5] * 100) \
        .astype('float64') \
        .round(2)
    abs_values = ads_df_merged.iloc[0, 1:5]

    # Текст отчета
    to_text = f"""
    Отчет по объявлению 121288 за 2 апреля
    Траты: {abs_values.ad_expenses} рублей ({diff_values.ad_expenses}%)
    Показы: {abs_values.number_of_view} ({diff_values.number_of_view}%)
    Клики: {abs_values.number_of_clicks} ({diff_values.number_of_clicks}%)
    CTR: {abs_values.ctr} ({diff_values.ctr}%)   
    """

    # Блок VK_API
    token = '1ec4d0b0c4e7dbfcbee25d61d593a4123757b48f23e1aeb51b78469e1b6f47e6b05d9b6b18ba9d0bef2d5'

    user_id = 601621779
    vk_session = vk_api.VkApi(token=token)
    vk = vk_session.get_api()

    vk.messages.send(
        user_id=user_id,
        random_id=random.randint(0, 2 ** 18),
        message=to_text,
    )


bash_operator = PythonOperator(
    task_id='send_report',
    python_callable=report_to_vk,
    dag=dag,
)
