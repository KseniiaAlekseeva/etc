from datetime import datetime
import os
import requests
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

dag=DAG('seminar8', description='Seminar 8 dag',
    schedule="0 12 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False
)

def get_weather_data(ti=None):
    openweather_url = "https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=f1b619dd8071df18a3fb895f4b2c06c8"
    response = requests.get(openweather_url)
    temp_openweather=round(response.json()['main']['temp']-273,1)
    ti.xcom_push(key='temperature_openweather', value=temp_openweather)

    yandex_url = "https://api.weather.yandex.ru/v2/forecast?lat=55.75396&lon=37.620393"
    headers = {"X-Yandex-API-Key": "afb813cb-9b60-4611-b2e1-daa16d14d790"}
    response = requests.get(url=yandex_url, headers=headers)
    temp_yandex = response.json()['fact']['temp']
    ti.xcom_push(key='temperature_yandex', value=temp_yandex)

get_weather=PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

send_tg_message=TelegramOperator(
    task_id='send_tg_message',
    telegram_conn_id='telegram_conn',
    token='7133784658:AAFoS4pVjOKs6GCKnQEUH7qmfon7YunD7wQ',
    chat_id=823563530,
    text='Temperature in London OpenWeather: \
        {{ ti.xcom_pull(key="temperature_openweather",task_ids="get_weather_data") }} \n \
        Temperature in London Yandex: \
        {{ ti.xcom_pull(key="temperature_yandex",task_ids="get_weather_data") }}',
    dag=dag
)

get_weather >> send_tg_message

#yandex afb813cb-9b60-4611-b2e1-daa16d14d790
#openweather f1b619dd8071df18a3fb895f4b2c06c8
#telegram 7133784658:AAFoS4pVjOKs6GCKnQEUH7qmfon7YunD7wQ
