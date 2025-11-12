import pendulum
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import csv

START_DATE = pendulum.datetime(2025, 10, 20, tz="UTC")

with DAG(
    dag_id="04_dag",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/data/"],
    tags=["tp_04"],
) as dag:
    
    def _extract_runners_query(output_folder:str):
        with open("/opt/airflow/data/extract_runners.sql", "w") as f:
            f.write(
                "SELECT * FROM runners;\n"
            )

    def _runners_query_handler(cursor):
        csv_path = "/opt/airflow/data/runners.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _insert_runners_query(output_folder:str):
        with open("/opt/airflow/data/insert_runners.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/runners.csv")
            f.write(
                "INSERT INTO DimRunner (RunnerKey, Name, Age, Genre, Nationality)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                runner_id = row.runnerid
                name = row.name
                age = row.age
                genre = row.genre
                nationality = row.nationality
                values.append(f"({runner_id}, '{name}', '{age}', '{genre}', '{nationality}')")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (RunnerKey) DO NOTHING;\n")
            f.write("SELECT setval('dimrunner_runnerkey_seq', COALESCE((SELECT MAX(RunnerKey) FROM DimRunner), 0),  true);\n")


    extract_runners_query = PythonOperator(
        task_id="extract_runners_query",
        python_callable=_extract_runners_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    extract_runners = SQLExecuteQueryOperator(
        task_id="extract_runners",
        conn_id="OLTP",
        sql="extract_runners.sql",
        autocommit=True,
        handler=_runners_query_handler,
    )

    insert_runners_query = PythonOperator(
        task_id="insert_runners_query",
        python_callable=_insert_runners_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_runners = SQLExecuteQueryOperator(
        task_id="insert_runners",
        conn_id="OLAP",
        sql="insert_runners.sql",
        autocommit=True,
        handler=None,
    )

    extract_runners_query >> extract_runners >> insert_runners_query >> insert_runners