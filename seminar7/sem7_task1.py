import datetime
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email_operator import EmailOperator

@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    create_employees_table = PostgresOperator(
        task_id="create_employees_table",
        postgres_conn_id="pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
            "Serial Number" NUMERIC PRIMARY KEY,
            "Company Name" TEXT,
            "Employee Markme" TEXT,
            "Description" TEXT,
            "Leave" INTEGER
            );""",
        )
    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="pg_conn",
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
            "Serial Number" NUMERIC PRIMARY KEY,
            "Company Name" TEXT,
            "Employee Markme" TEXT,
            "Description" TEXT,
            "Leave" INTEGER
            );""",
        )
    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment data_path = PATH_TO_FOLDER" os.makedirs(os.path.dirname(data_path), exist_ok=True)
        data_path="/usr/local/airflow/data/data.csv"
        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        response = requests.request("GET", url)
        with open(data_path, "w") as file:
            file.write(response.text)
            postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
            conn.commit()
            
    @task
    def merge_data(ti=None):
        count_q="""
                SELECT COUNT(*) FROM employees;
                """
        query = """
            INSERT INTO employees SELECT *
            FROM (
            SELECT DISTINCT *
            FROM employees_temp
            ) AS emp
            ON CONFLICT ("Serial Number") DO UPDATE
            SET "Serial Number" = excluded."Serial Number"; """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(count_q)
            res_before=int(cur.fetchone()[0])
            cur.execute(query)
            conn.commit()
            cur.execute(count_q)
            res_after=int(cur.fetchone()[0])
            ti.xcom_push(key="rows",value=res_after-res_before)
        except Exception as e:
            ti.xcom_push(key="rows",value=0)

    @task
    def send_email(ti=None):
        number_of_added_rows=ti.xcom_pull(task_ids="merge_data", key="rows")
        print(f'Number of added rows - {number_of_added_rows}')
        if number_of_added_rows>0:
            email='csusha-aleks@yandex.ru'
            msg=f"Added {number_of_added_rows} rows."
            subj='Adding rows'
            EmailOperator(task_id='send_email',to=email,subject=subj, html_content=msg)
            
    
    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data() >> send_email()

dag = ProcessEmployees()
