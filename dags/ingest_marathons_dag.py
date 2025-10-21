import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo.errors import BulkWriteError

START_DATE = pendulum.datetime(2025, 10, 20, tz="UTC")

with DAG(
    dag_id="ingest_marathons",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    template_searchpath=["/opt/airflow/data/"],
    tags=["ingestion"]
) as dag :

    def insert_marathons_data(**context):
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['major_marathons']
        collection.create_index("ID", unique=True)

        df = pd.read_csv("/opt/airflow/data/major_marathons_data.csv")
        data_to_insert = df.to_dict('records')
        try:
            collection.insert_many(data_to_insert, ordered=False)
            print(f"{len(data_to_insert)} inserted documents into 'major_marathons' collection.")
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup_count = sum(1 for err in write_errors if err.get("code") == 11000)
            total_inserts = len(data_to_insert) - dup_count
            print(f"{dup_count} detected and ignored duplicates.")
            print(f"{total_inserts} new inserted documents into 'major_marathons' collection.")
        
        client.close()

    get_spreadsheet = BashOperator(
        task_id="get_spreadsheet",
        bash_command=(
            "curl -fsSL https://raw.githubusercontent.com/ali-ce/datasets/master/Marathon-Majors/Races.csv "
            "--output /opt/airflow/data/major_marathons_data.csv"
        )
    )

    insert_marathons = PythonOperator(
        task_id='insert_marathons_data',
        python_callable=insert_marathons_data
    )

    get_spreadsheet >> insert_marathons