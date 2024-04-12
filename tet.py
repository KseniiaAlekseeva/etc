import os

import requests
def get_data():
    # NOTE: configure this as appropriate for your airflow environment data_path = PATH_TO_FOLDER" os.makedirs(os.path.dirname(data_path), exist_ok=True)
    data_path = "../data/data.csv"
    url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
    response = requests.request("GET", url)
    print(os.getcwd())
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

get_data()