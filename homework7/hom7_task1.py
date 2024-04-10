from datetime import datetime
import random
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

dag=DAG('homework7', description='Homework7 dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 1, 1),
          catchup=False)

def get_weather(city, ti=None):
    weatherkey = 'f1b619dd8071df18a3fb895f4b2c06c8'
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {'appid' : weatherkey, 'q' : city, 'units' : 'metric'}
    response = requests.get(url, params = params)
    temp = round(response.json()['main']['temp'],1)
    ti.xcom_push(key='temperature', value=temp)
    return temp

def cold_or_warm(ti):
    temp=ti.xcom_pull(key="temperature",task_ids="get_weather_task")
    print(str(temp) + '°C')
    if float(temp)>15:
        return 'warm_task'
    return 'cold_task'

get_weather=PythonOperator(
    task_id='get_weather_task',
    python_callable=get_weather,
    op_kwargs={'city':'Moscow'},
    dag=dag
)
cold_or_warm=BranchPythonOperator(
    task_id='cold_or_warm_task',
    python_callable=cold_or_warm,
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



