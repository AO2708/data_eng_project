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
import os

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
            city = os.path.basename(filename).split("_weather.json")[0]
            with open(filename, 'r') as f:
                data = json.load(f)
                df = pd.json_normalize(data)
                df["ID"] = city
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

        start = datetime(2000, 1, 1)
        end = datetime(2019, 12, 31)

        print(start,end)

        for city_name, location in cities.items():
            filename = "/opt/airflow/data/" + city_name.lower() + "_weather.json"
            try :
                data = Daily(location, start, end)
                data = data.fetch()
                try:
                    os.remove(filename)
                    print(f"File '{filename}' has been deleted successfully.")
                except FileNotFoundError:
                    print(f"File '{filename}' not found.")
                except PermissionError:
                    print(f"Permission denied to delete the file '{filename}'.")
                except Exception as e:
                    print(f"Error occurred while deleting the file: {e}")

                data.to_json(filename)
                print(f"Saved data for {city_name} in {filename}")
            except Exception as e:
                print(f"[WARNING] Unable to fetch data for {city_name}: {e}")
                print(f"[INFO] Keeping existing file '{filename}'")

    run_weather_script = PythonOperator(
        task_id="run_weather_script",
        python_callable=fetch_weather_data
    )

    insert_weather = PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data
    )

    run_weather_script >> insert_weather