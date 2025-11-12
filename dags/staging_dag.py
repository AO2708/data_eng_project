import pendulum
import pandas as pd
from meteostat import Point, Daily
from datetime import datetime
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
from pymongo.errors import BulkWriteError

START_DATE = pendulum.datetime(2025, 10, 20, tz="UTC")

with DAG(
    dag_id="staging_data",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    template_searchpath=["/opt/airflow/data/"],
    tags=["staging"]
) as dag :
    

    
    def clean_weather_data():
    
        cities = {
            "Boston": Point(42.361145, -71.057083)
        }

        start = datetime(2010, 1, 1)
        end = datetime(2010, 1, 2)

        for city_name, location in cities.items():
            # Récupération des données Meteostat
            data = Daily(location, start, end).fetch()

            # Convertir l'index en datetime aware UTC
            data.index = pd.to_datetime(data.index, unit='ms', utc=True)

            # Transformer l'index en colonne 'date' et convertir en chaîne
            daily_list = data.copy()
            daily_list['date'] = daily_list.index.astype(str)  # <-- correction ici
            daily_list = daily_list.reset_index(drop=True)

            # Transformer en liste de dictionnaires
            daily_list = daily_list.to_dict(orient='records')

            # Sauvegarde en JSON
            filename = city_name.lower() + "_weather.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(daily_list, f, ensure_ascii=False, indent=2)

            print(f"Saved data for {city_name} in {filename}")

    clean_weather = PythonOperator(
        task_id=f'clean_weather_data',
        python_callable=clean_weather_data
    )

    aggregate_marathons_date = PostgresOperator (

    )

    mathieu = PostgresOperator (
        
    )


    [clean_weather, aggregate_marathons_date] >> mathieu