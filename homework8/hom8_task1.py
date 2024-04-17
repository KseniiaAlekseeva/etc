import datetime
import os
import requests
import pendulum
import pandas as pd
from sqlalchemy.orm import sessionmaker
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id='homework8',
    description='Homework 8 dag',
    schedule_interval="0 12 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def LoadBookings():
    def get_data(file: str):
        data = pd.read_csv(file)
        return data

    get_client=PythonOperator(
        task_id='get_client',
        python_callable=lambda:get_data('/usr/local/airflow/data/client.csv')
        )
    get_hotel=PythonOperator(
        task_id='get_hotel',
        python_callable=lambda:get_data('/usr/local/airflow/data/hotel.csv')
        )
    get_booking=PythonOperator(
        task_id='get_booking',
        python_callable=lambda:get_data('/usr/local/airflow/data/booking.csv')
        )

    def corr_date(s:str):
        if '-' in s:
            return datetime.datetime.strptime(s, '%Y-%m-%d')
        if '/' in s:
            return datetime.datetime.strptime(s, '%Y/%m/%d')

    def curr_rate(cur:str):
        fin_cur = 'RUB'
        api_key = 'fbd24b58b1415f0ec7a165dd'
        url = f'https://v6.exchangerate-api.com/v6/{api_key}/latest/{fin_cur}'
        response = requests.get(url)
        try:
            rate = response.json()['conversion_rates'][cur]
            return rate, fin_cur
        except Exception as e:
            return None, cur
    
    @task
    def corr_data(ti):
        df = ti.xcom_pull(task_ids='get_booking')
        df['booking_date']=df['booking_date'].apply(corr_date)
        df['booking_cost'] = df['booking_cost'] / df['currency'].apply(lambda x: curr_rate(x)[0])
        df['currency'] = df['currency'].apply(lambda x: curr_rate(x)[1])
        return df


    @task
    def load_tables(ti):
        df_cl = ti.xcom_pull(task_ids='get_client')
        df_ho = ti.xcom_pull(task_ids='get_hotel')
        df_bo = ti.xcom_pull(task_ids='corr_data')
        
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        df_cl.to_sql('client', postgres_hook.get_sqlalchemy_engine(), if_exists='replace')
        df_ho.to_sql('hotel', postgres_hook.get_sqlalchemy_engine(), if_exists='replace')
        df_bo.to_sql('booking', postgres_hook.get_sqlalchemy_engine(), if_exists='replace')

    @task
    def merge_data():
        query="""
            DROP TABLE IF EXISTS total_booking;
            CREATE TABLE total_booking
            AS
            SELECT booking.booking_date,
                booking.room_type,
                booking.booking_cost,
                booking.currency,
                hotel.hotel_id,
                hotel.name AS hotel_name,
                hotel.address, 
                client.client_id,
                client.age AS client_age, 
                client.name AS client_name,
                client.type AS client_type 
            FROM booking
            INNER JOIN hotel
            ON booking.hotel_id=hotel.hotel_id
            INNER JOIN client
            ON booking.client_id=client.client_id;
        """
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
    [get_client, get_hotel, get_booking] >> corr_data() >> load_tables() >> merge_data()

LoadBookings()

