import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo.errors import BulkWriteError
import glob
import json
from meteostat import Point, Daily
import pandas as pd
from datetime import datetime

START_DATE = pendulum.datetime(2025, 10, 20, tz="UTC")

with DAG(
    dag_id="ingest_weather",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    template_searchpath=["/opt/airflow/data/"],
    tags=["ingestion"]
) as dag :

    def insert_weather_data(**context):
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['weather_data']
        collection.create_index("ID", unique=True)

        all_files = glob.glob("/opt/airflow/data/*_weather.json")
        df_list = []
        for filename in all_files:
            with open(filename, 'r') as f:
                data = json.load(f)
                df = pd.json_normalize(data)
                df_list.append(df)
        merged_df = pd.concat(df_list, ignore_index=True)
        data_to_insert = merged_df.to_dict(orient='records')

        try:
            collection.insert_many(data_to_insert, ordered=False)
            print(f"{len(data_to_insert)} inserted documents into 'weather_data' collection.")
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup_count = sum(1 for err in write_errors if err.get("code") == 11000)
            total_inserts = len(data_to_insert) - dup_count
            print(f"{dup_count} detected and ignored duplicates.")
            print(f"{total_inserts} new inserted documents into 'weather_data' collection.")
        
        client.close()

    def fetch_weather_data():

        cities = {
            "Boston": Point(42.361145, -71.057083)
        }

        start = datetime(2010, 1, 1)
        end = datetime(2010, 12, 31)

        for city_name, location in cities.items():
            data = Daily(location, start, end)
            data = data.fetch()

            filename = "/opt/airflow/data/" + city_name.lower() + "_weather.json"
            data.to_json(filename)
            print(f"Saved data for {city_name} in {filename}")


    run_weather_script = PythonOperator(
        task_id="run_weather_script",
        python_callable=fetch_weather_data
    )

    insert_weather = PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data
    )

    run_weather_script >> insert_weather