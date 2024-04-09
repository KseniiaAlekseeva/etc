from datetime import datetime
import random
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

def _get_weather(city):
    weatherkey = "f1b619dd8071df18a3fb895f4b2c06c8"
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {'APPID' : weatherkey, 'q' : city, 'units' : 'metric'}
    response = requests.get(url, params = params)
    weather = response.json()
    temp = weather['main']['temp']
    ti.xcom_push(key='temperature', value=temp)
    temp=22
    return temp

def _cold_or_warm(ti):
    temp=ti.xcom_pull(task_ids='weather_operator')
    print(str(round(temp,1)) + '°C')
    if temp>15:
        return 'warm'
    return 'cold'

dag=DAG('home7', description='Homework7 dag',
          schedule_interval='* * * * *',
          start_date=datetime(2023, 1, 1),
          catchup=False)

get_weather=PythonOperator(
    task_id='get_weather_task',
    python_callable=_get_weather,
    op_kwargs={'city':"Moscow"},
    dag=dag
    )
cold_or_warm=BranchPythonOperator(
    task_id='cold_or_warm_task',
    python_callable=_cold_or_warm,
    dag=dag
    )
cold = BashOperator(
    task_id="cold_task",
    bash_command="echo 'cold'"
    )
warm = BashOperator(
    task_id="warm_task",
    bash_command=" echo 'warm'"
    )

get_weather >> cold_or_warm >> [cold,warm]



