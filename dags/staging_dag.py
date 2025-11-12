import pendulum
import pandas as pd
from meteostat import Point, Daily
import csv
from datetime import datetime
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
    
    def get_boston_data():
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['boston_marathons']

        documents = list(collection.find())

        # 3. If no data, exit early
        if not documents:
            print("No documents found in collection.")
        else:
            # 4. Extract field names (keys)
            fieldnames = list(documents[0].keys())
            
            # Optional: remove MongoDB’s internal "_id" if not needed
            if "_id" in fieldnames:
                fieldnames.remove("_id")

            # 5. Write to CSV
            with open("/opt/airflow/data/boston_data.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for doc in documents:
                    # Remove _id or convert ObjectId to string if needed
                    doc.pop("_id", None)
                    writer.writerow(doc)

        print("✅ Collection exported to boston_data.csv")

        client.close()

    def get_marathons_date_data():
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['major_marathons_date']

        documents = list(collection.find())

        # 3. If no data, exit early
        if not documents:
            print("No documents found in collection.")
        else:
            # 4. Extract field names (keys)
            fieldnames = list(documents[0].keys())
            
            # Optional: remove MongoDB’s internal "_id" if not needed
            if "_id" in fieldnames:
                fieldnames.remove("_id")

            # 5. Write to CSV
            with open("/opt/airflow/data/boston_date.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for doc in documents:
                    # Remove _id or convert ObjectId to string if needed
                    doc.pop("_id", None)
                    writer.writerow(doc)

        print("✅ Collection exported to boston_date.csv")

        client.close()

    def get_weather_data():
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['weather_data']

        documents = list(collection.find())

        # 3. If no data, exit early
        if not documents:
            print("No documents found in collection.")
        else:
            # 4. Extract field names (keys)
            fieldnames = list(documents[0].keys())
            
            # Optional: remove MongoDB’s internal "_id" if not needed
            if "_id" in fieldnames:
                fieldnames.remove("_id")

            # 5. Write to CSV
            with open("/opt/airflow/data/weather_data.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for doc in documents:
                    # Remove _id or convert ObjectId to string if needed
                    doc.pop("_id", None)
                    writer.writerow(doc)

        print("✅ Collection exported to weather_data.csv")

        client.close()
    
    def clean_weather_data():
        input_file = "/opt/airflow/data/weather_data.csv"
        output_file = "/opt/airflow/data/cleaned_weather_data.csv"

        # Read the wide-format CSV
        df = pd.read_csv(input_file)

        # Transpose: columns (e.g. "tavg.1262304000000") become rows
        df = df.transpose().reset_index()
        df.columns = ["variable", "value"]

        # Split "variable" column into parameter (e.g. tavg) and timestamp (e.g. 1262304000000)
        df[["parameter", "timestamp"]] = df["variable"].str.split(".", expand=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(float), unit="ms", utc=True)

        # Pivot the data so that each timestamp becomes one row with all weather parameters
        df_clean = df.pivot(index="timestamp", columns="parameter", values="value").reset_index()

        # Rename timestamp column to "date"
        df_clean.rename(columns={"timestamp": "date"}, inplace=True)

        # Convert numeric columns properly
        for col in df_clean.columns:
            if col != "date":
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

        # Save to CSV
        df_clean.to_csv(output_file, index=False, encoding="utf-8")

        print(f"✅ Cleaned weather data saved to: {output_file}")

    def clean_boston_data():
        date_filename = "/opt/airflow/data/marathons_date.csv"
        boston_filename = "/opt/airflow/data/boston_data.csv"

        marathons_filename = "/opt/airflow/data/table_marathons.csv"
        result_filename = "/opt/airflow/data/table_result.csv"
        runners_filename = "/opt/airflow/data/table_runners.csv"

        df_date = pd.read_csv(date_filename)
        df_boston = pd.read_csv(boston_filename)
        df_boston.drop(index=df_boston.index[-1],axis=0,inplace=True)
        
        df_marathon = df_boston[["edition"]].copy()
        df_marathon.drop_duplicates(subset="edition", inplace=True)
        df_marathon.reset_index(drop=True, inplace=True)
        df_marathon.rename(columns={'edition': 'year'}, inplace=True)

        df_date["marathon"].str.replace(' Marathon', '', regex=False)
        df_date.rename(columns={'marathon': 'city'}, inplace=True)

        df_date['date'] = pd.to_datetime(df_date['date'])

        # Extract year, month, and day
        df_date['year'] = df_date['date'].dt.year
        df_date['month'] = df_date['date'].dt.month
        df_date['day'] = df_date['date'].dt.day

        # Merge only matching years from df_year
        df_final = df_marathon.merge(df_date, on='year', how='left')

        df_final = df_final.rename(columns={'date': 'full_date'})
        df_final = df_final[['city', 'full_date', 'year', 'month', 'day']]
        df_final.drop_duplicates(inplace=True)
        df_final.reset_index(drop=True, inplace=True)

        df_final.to_csv(marathons_filename, index=False)

        df_result = df_boston[["place_overall","official_time","gender_result","edition","display_name"]]
        df_result = df_result.rename(columns={'place_overall': 'ranking','official_time':'time', 'edition':'year','display_name':'name'})

        for index, row in df_result.iterrows():
            # skip if time is missing
            if pd.isna(row["time"]):
                df_result.at[index, 'pace'] = None
                continue

            parts = row["time"].split(':')
            # If format is MM:SS, prepend 0 hours
            if len(parts) == 2:
                h, m, s = 0, int(parts[0]), int(parts[1])
            elif len(parts) == 3:
                h, m, s = map(int, parts)
            else:
                # Unexpected format
                df_result.at[index, 'pace'] = None
                continue

            pace_seconds = (int(h) * 3600 + int(m) * 60 + int(s)) / 42.195
            df_result.at[index, 'pace'] = f"{int(pace_seconds // 3600):02d}:{int((pace_seconds % 3600) // 60):02d}:{int(pace_seconds % 60):02d}"

        df_result = df_result.dropna()
        df_result.to_csv(result_filename, index=False)

        df_runner = df_boston[["display_name","age","gender","contry_citizenship"]]
        df_runner = df_runner.rename(columns={'display_name':'name','contry_citizenship':'citizenship'})
        df_runner.drop_duplicates(inplace=True)
        df_runner.reset_index(drop=True, inplace=True)
        
        df_runner = df_runner.dropna()
        df_runner.to_csv(runners_filename, index=False)

    def sort_weather_data():
        weather_filename = "/opt/airflow/data/cleaned_weather_data.csv"
        date_filename = "/opt/airflow/data/table_marathons.csv"

        output_filename = "/opt/airflow/data/table_weather.csv"

        df_weather = pd.read_csv(weather_filename)
        df_date = pd.read_csv(date_filename)

        df_date['full_date'] = pd.to_datetime(df_date['full_date']).dt.date
        df_weather['date'] = pd.to_datetime(df_weather['date']).dt.date

        weather_cols = ['date', 'prcp', 'pres', 'tsun', 'tavg', 'wspd', 'snow']
        df_weather_sel = df_weather[weather_cols]
        
        df_final = df_weather_sel[df_weather_sel['date'].isin(df_date['full_date'])].reset_index(drop=True)

        # Save to CSV
        df_final.to_csv(output_filename, index=False)


    clean_weather = PythonOperator(
        task_id=f'clean_weather_data',
        python_callable=clean_weather_data
    )


    mathieu = SQLExecuteQueryOperator (
        task_id="mathieu",
        conn_id="postgres_default",
        sql="test.sql",
        autocommit=True,
    )

    get_boston = PythonOperator(
            task_id=f'get_boston_data',
            python_callable=get_boston_data
        )

    get_marathons_date = PythonOperator(
            task_id=f'get_marathons_date_data',
            python_callable=get_marathons_date_data
        )
    
    get_weather = PythonOperator(
        task_id=f'get_weather_data',
        python_callable=get_weather_data
    )

    clean_boston = PythonOperator(
        task_id=f"clean_boston_data",
        python_callable=clean_boston_data
    )

    sort_weather = PythonOperator (
        task_id=f"sort_weather_data",
        python_callable=sort_weather_data
    )

    ## get_boston >> get_marathons_date >> get_weather >> clean_weather >> 
    clean_boston >> sort_weather
