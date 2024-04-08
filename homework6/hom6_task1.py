from datetime import datetime
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator 

def print_rand(min_val, max_val):
    num=random.randint(min_val, max_val)
    num2=num*num
    print(num,num2)

dag=DAG('hom6_dag', description='Homework6 dag',
          schedule_interval='* * * * *',
          start_date=datetime(2023, 1, 1),
          catchup=False)

bash_random_operator=BashOperator(
    task_id='bash_random_task',
    bash_command='echo $RANDOM',
    dag=dag
    )
#python_random_operator=PythonOperator(
#    task_id='python_random_task',
#    python_callable=print_rand,
#    op_args=[0,10],
#    dag=dag
#    )
#python_random_operator=PythonOperator(
#    task_id='python_random_task',
#    python_callable=print_rand,
#    op_kwargs={'min_val':0,'max_val':10},
#    dag=dag
#    )
python_random_operator=PythonOperator(
    task_id='python_random_task',
    python_callable=lambda:print_rand(0,10),
    dag=dag
    )
http_operator = SimpleHttpOperator( 
        task_id='http_task', 
        http_conn_id='ya_site', 
        endpoint='/moscow', 
        method='GET' 
    )

bash_random_operator >> python_random_operator >> http_operator
