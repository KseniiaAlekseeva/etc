from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

dag = DAG('hello_world', description='Hello world DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 1, 1),
          catchup=False)

hello_operator=BashOperator(task_id='hello_task',bash_command='echo Hello from Airflow', dag=dag)
skip_operator=BashOperator(task_id='skip_task',bash_command='exit 99',dag=dag)
hello_file_operator=BashOperator(task_id='hello_file_task',bash_command='scripts/file1.sh',dag=dag)

#hello_operator>> skip_operator >> hello_file_operator
hello_operator>> [skip_operator,hello_file_operator]
