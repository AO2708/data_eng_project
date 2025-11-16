import pendulum
import pandas as pd
import csv
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
    
    def _check_db_handler(cursor):
        row = cursor.fetchone()
        if row is not None:
            return True
        else: 
            return False
        
    def _branch_on_db_existence(ti):
        db_exists = ti.xcom_pull(task_ids="check_db")
        if db_exists:
            return "skip_create"
        else:
            return "create_db"
    
    def _get_boston():
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

    def _get_marathons_date():
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

    def _get_weather():
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
    
    def _clean_weather():
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

    def _clean_boston():
        # Preparation
        date_filename = "/opt/airflow/data/boston_date.csv"
        boston_filename = "/opt/airflow/data/boston_data.csv"

        marathons_filename = "/opt/airflow/data/table_marathons.csv"
        result_filename = "/opt/airflow/data/table_result.csv"
        runners_filename = "/opt/airflow/data/table_runners.csv"

        df_date = pd.read_csv(date_filename)
        df_boston = pd.read_csv(boston_filename)
        df_boston.drop(index=df_boston.index[-1],axis=0,inplace=True)

        # Cleaning of df_boston
        for index, row in df_boston.iterrows():
            if row["contry_citizenship"] == "United ":
                df_boston.at[index, "contry_citizenship"] = "USA"
            if row["country_residence"] == "United ":
                df_boston.at[index, "country_residence"] = "USA"
            if pd.isna(row["contry_citizenship"]) and not pd.isna(row["country_residence"]):
                df_boston.at[index, "contry_citizenship"] = row["country_residence"]

            if pd.isna(row["official_time"]):
                df_boston.at[index, 'pace'] = None
                continue
            parts = row["official_time"].split(':')
            if len(parts) == 2:
                h, m, s = 0, int(parts[0]), int(parts[1])
            elif len(parts) == 3:
                h, m, s = map(int, parts)
            else:
                df_boston.at[index, 'pace'] = None
                continue
            if m >= 60 or s >= 60:
                df_boston.at[index, 'pace'] = None
                continue

            pace_seconds = (int(h) * 3600 + int(m) * 60 + int(s)) / 42.195
            df_boston.at[index, 'pace'] = f"{int(pace_seconds // 3600):02d}:{int((pace_seconds % 3600) // 60):02d}:{int(pace_seconds % 60):02d}"

        keep_columns = ["place_overall","official_time","gender_result","edition","display_name","age","gender","contry_citizenship","pace"]
        df_boston = df_boston[keep_columns]
        df_boston = df_boston.dropna()

        # table_results.csv
        df_result = df_boston[["place_overall","official_time","gender_result","edition","display_name","pace"]]
        df_result = df_result.rename(columns={'place_overall': 'ranking','official_time':'time', 'edition':'year','display_name':'name'})
        df_result.to_csv(result_filename, index=False)

        # table_runners.csv
        df_runner = df_boston[["display_name","age","gender","contry_citizenship"]]
        df_runner = df_runner.rename(columns={'display_name':'name','contry_citizenship':'nationality'})
        df_runner.drop_duplicates(inplace=True)
        df_runner.reset_index(drop=True, inplace=True)
        df_runner.to_csv(runners_filename, index=False)

        # table_marathons.csv
        df_marathon = df_boston[["edition"]].copy()
        df_marathon.drop_duplicates(subset="edition", inplace=True)
        df_marathon.reset_index(drop=True, inplace=True)
        df_marathon.rename(columns={'edition': 'year'}, inplace=True)

        df_date["marathon"].str.replace(' Marathon', '', regex=False)
        df_date.rename(columns={'marathon': 'city'}, inplace=True)

        df_date['date'] = pd.to_datetime(df_date['date'])

        df_date['year'] = df_date['date'].dt.year
        df_date['month'] = df_date['date'].dt.month
        df_date['day'] = df_date['date'].dt.day

        df_final = df_marathon.merge(df_date, on='year', how='left')

        df_final = df_final.rename(columns={'date': 'full_date'})
        df_final = df_final[['city', 'full_date', 'year', 'month', 'day']]
        df_final.drop_duplicates(inplace=True)
        df_final.reset_index(drop=True, inplace=True)

        df_final.to_csv(marathons_filename, index=False)

    def _sort_weather():
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

    # Create table query
    def _create_tables_query():
        with open("/opt/airflow/data/create_tables_staging.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS Marathons(\n"
                "   marathon_id SERIAL PRIMARY KEY,\n"
                "   city VARCHAR(100),\n"
                "   full_date DATE NOT NULL,\n"
                "   year INT,\n"
                "   month INT,\n"
                "   day INT\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS Runners(\n"
                "   runner_id SERIAL PRIMARY KEY,\n"
                "   Name VARCHAR(100),\n"
                "   Age INT,\n"
                "   Gender VARCHAR(1),\n"
                "   Nationality VARCHAR(50)\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS Weather(\n"
                "   weather_id SERIAL PRIMARY KEY,\n"
                "   t_avg NUMERIC(3,1),\n"
                "   precipitation NUMERIC(3,1),\n"
                "   snow NUMERIC(4,1),\n"
                "   wind_speed NUMERIC(4,1),\n"
                "   pressure NUMERIC(5,1),\n"
                "   sun NUMERIC(5,1),\n"
                "   marathon_id INT REFERENCES Marathons (marathon_id)\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS Result(\n"
                "   ranking INT,\n"
                "   time Time,\n"
                "   pace Time,\n"
                "   gender_result INT,\n"
                "   marathon_id INT REFERENCES Marathons (marathon_id),\n"
                "   runner_id INT REFERENCES Runners (runner_id)\n"
                ");\n"
            )

    # Insert queries

    def _insert_runners_query(output_folder:str):
        with open("/opt/airflow/data/insert_runners_staging.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/table_runners.csv")
            f.write(
                "INSERT INTO Runners (name, age, gender, nationality)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                name = row.name.replace("'", "")
                age = row.age
                gender = row.gender
                nationality = row.nationality
                values.append(f"('{name}', {age}, '{gender}', '{nationality}')")
            f.write(", \n".join(values) + ";")

    def _insert_marathons_query(output_folder:str):
        with open("/opt/airflow/data/insert_marathons_staging.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/table_marathons.csv")
            f.write(
                "INSERT INTO Marathons (city, full_date, year, month, day)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) : 
                city = row.city
                full_date = row.full_date
                year = row.year
                month = row.month
                day = row.day
                values.append(f"('{city}', '{full_date}', '{year}', '{month}', '{day}')")
            f.write(", \n".join(values) + ";")

    def _insert_result_query(output_folder:str, ti):
        marathon_dict = ti.xcom_pull(task_ids='get_marathons_id')
        runner_dict = ti.xcom_pull(task_ids='get_runners_id')
        with open("/opt/airflow/data/insert_result_staging.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/table_result.csv", thousands=',')
            f.write(
                "INSERT INTO Result (marathon_id, runner_id, ranking, time, pace, gender_result)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) : 
                year = row.year
                name = row.name.replace("'","")
                ranking = int(row.ranking)
                time = row.time
                pace = row.pace
                gender_result = int(row.gender_result)
                marathon_id = marathon_dict.get(str(year))
                runner_id = runner_dict.get(name)
                values.append(
                    "("
                    f"{marathon_id}, "
                    f"{runner_id}, "
                    f"{ranking}, '{time}', '{pace}', {gender_result}"
                    ")"
                )
            f.write(", \n".join(values) + ";")

    def _insert_weather_query(output_folder:str):
        with open("/opt/airflow/data/insert_weather_staging.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/table_weather.csv")
            f.write(
                "INSERT INTO Weather (marathon_id, t_avg, precipitation, pressure, snow, wind_speed, sun)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) : 
                full_date = row.date
                t_avg = row.tavg
                precipitation = row.prcp
                pressure = row.pres
                snow = row.snow
                wind_speed = row.wspd
                sun = row.tsun
                
                values.append(
                    "("
                    f"(SELECT marathon_id FROM Marathons WHERE full_date = '{full_date}'), "
                    f"{t_avg}, {precipitation}, {pressure}, {snow}, "
                    f"{wind_speed}, {sun}"
                    ")"
                )
            f.write(", \n".join(values) + ";")

    def _store_marathons_id(cursor):
        rows = cursor.fetchall()
        marathon_dict = {int(row[0]): int(row[1]) for row in rows}
        return marathon_dict
    
    def _store_runners_id(cursor):    
        rows = cursor.fetchall()
        runner_dict = {str(row[0]): int(row[1]) for row in rows}
        return runner_dict

    
    # Operators

    check_db = SQLExecuteQueryOperator(
        task_id="check_db",
        conn_id="postgres_default",
        sql="SELECT 1 FROM pg_database WHERE datname = 'staging';",
        autocommit=True,
        handler=_check_db_handler,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_db_existence",
        python_callable=_branch_on_db_existence,
    )

    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="postgres_default",
        sql="CREATE DATABASE staging;",
        autocommit=True,
        handler=None
    )

    skip_create = EmptyOperator(
        task_id="skip_create"
    )

    create_tables_query = PythonOperator(
        task_id="create_tables_query",
        python_callable=_create_tables_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="postgres_staging",
        sql="create_tables_staging.sql",
        autocommit=True,
        handler=None,
    )

    insert_runners_query = PythonOperator(
        task_id="insert_runners_query",
        python_callable=_insert_runners_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_marathons_query = PythonOperator(
        task_id="insert_marathons_query",
        python_callable=_insert_marathons_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_result_query = PythonOperator(
        task_id="insert_result_query",
        python_callable=_insert_result_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_weather_query = PythonOperator(
        task_id="insert_weather_query",
        python_callable=_insert_weather_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_runners = SQLExecuteQueryOperator(
        task_id="insert_runners",
        conn_id="postgres_staging",
        sql="insert_runners_staging.sql",
        autocommit=True,
        handler=None,
    )

    insert_marathons = SQLExecuteQueryOperator(
        task_id="insert_marathons",
        conn_id="postgres_staging",
        sql="insert_marathons_staging.sql",
        autocommit=True,
        handler=None,
    )

    get_marathons_id = SQLExecuteQueryOperator(
        task_id="get_marathons_id",
        conn_id="postgres_staging",
        sql="SELECT year, marathon_id FROM Marathons;",
        autocommit=True,
        handler=_store_marathons_id,
    )

    get_runners_id = SQLExecuteQueryOperator(
        task_id="get_runners_id",
        conn_id="postgres_staging",
        sql="SELECT name, runner_id FROM Runners;",
        autocommit=True,
        handler=_store_runners_id,
    )

    insert_result = SQLExecuteQueryOperator(
        task_id="insert_result",
        conn_id="postgres_staging",
        sql="insert_result_staging.sql",
        autocommit=True,
        handler=None,
    )

    insert_weather = SQLExecuteQueryOperator(
        task_id="insert_weather",
        conn_id="postgres_staging",
        sql="insert_weather_staging.sql",
        autocommit=True,
        handler=None,
    )
    
    
    clean_weather = PythonOperator(
        task_id='clean_weather',
        python_callable=_clean_weather
    )

    get_boston = PythonOperator(
            task_id='get_boston',
            python_callable=_get_boston
        )

    get_marathons_date = PythonOperator(
            task_id='get_marathons_date',
            python_callable=_get_marathons_date
        )
    
    get_weather = PythonOperator(
        task_id='get_weather',
        python_callable=_get_weather
    )

    clean_boston = PythonOperator(
        task_id="clean_boston",
        python_callable=_clean_boston
    )

    sort_weather = PythonOperator (
        task_id="sort_weather",
        python_callable=_sort_weather
    )

    merge = EmptyOperator(
        task_id="merge",
        trigger_rule="none_failed",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed",
    )

    check_db >> branch >> [create_db, skip_create] >> create_tables_query
    create_tables_query >> create_tables >> [get_boston, get_marathons_date, get_weather]
    [get_boston, get_marathons_date, get_weather] >> clean_weather >> clean_boston >> sort_weather 
    sort_weather >> [insert_runners_query, insert_marathons_query]
    insert_runners_query >> insert_runners
    insert_marathons_query >> insert_marathons
    [insert_runners, insert_marathons] >> merge
    merge >> [get_marathons_id, insert_weather_query]
    get_marathons_id >> get_runners_id >> insert_result_query >> insert_result
    insert_weather_query >> insert_weather
    [insert_result, insert_weather] >> end
