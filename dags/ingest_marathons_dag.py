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

    def insert_marathons_data(year):
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['boston_marathons']
        collection.create_index("ID", unique=True)

        df = pd.read_csv(f"/opt/airflow/data/{year}_marathons_data.csv", on_bad_lines='skip')
        df["edition"] = year
        df["ID"] = df["overall"].astype(str) + "_" + df["edition"].astype(str)
        data_to_insert = df.to_dict('records')
        try:
            collection.insert_many(data_to_insert, ordered=False)
            print(f"{len(data_to_insert)} inserted documents into 'major_marathons' collection for {year} Boston marathon.")
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup_count = sum(1 for err in write_errors if err.get("code") == 11000)
            total_inserts = len(data_to_insert) - dup_count
            print(f"{dup_count} detected and ignored duplicates for {year} Boston marathon.")
            print(f"{total_inserts} new inserted documents into 'major_marathons' collection for {year} Boston marathon.")

        client.close()

    for year in range(2000, 2020):

        get_spreadsheet = BashOperator(
            task_id=f"get_spreadsheet_{year}",
            bash_command=(
                f"curl -fsSL https://raw.githubusercontent.com/adrian3/Boston-Marathon-Data-Project/refs/heads/master/results{year}.csv "
                f"--output /opt/airflow/data/{year}_marathons_data.csv ||"
                f"echo '[WARNING] Failed to download {year} data, keeping existing file if present'"
            )
        )

        insert_marathons = PythonOperator(
            task_id=f'insert_marathons_data_{year}',
            python_callable=insert_marathons_data,
            op_kwargs={'year': year}
        )

        get_spreadsheet >> insert_marathons