import pendulum
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import csv

START_DATE = pendulum.datetime(2025, 10, 20, tz="UTC")

with DAG(
    dag_id="production_dag",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    template_searchpath=["/opt/airflow/data/"],
    tags=["production"]
) as dag:

    def _check_db_handler(cursor):
        """
        Check whether the cursor returned at least one row meaning the production database already exists.
        :param cursor: A database cursor object.
        :return: True if a row exists, otherwise False.
        """
        row = cursor.fetchone()
        if row is not None:
            return True
        else:
            return False
        
    def _branch_on_db_existence(ti):
        """
        Branch depending on whether the production database exists.
        :param ti: Airflow TaskInstance used to pull XCom values.
        :return: The id of the next task to execute.
        """
        db_exists = ti.xcom_pull(task_ids="check_db")
        if db_exists:
            return "skip_create"
        else:
            return "create_db"

    # Extract queries
    def _create_tables_query(output_folder:str):
        """
        Create a SQL file containing queries in order to create the database tables if they do not exist.
        """
        with open("/opt/airflow/data/create_tables.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS DimWeather(\n"
                "   WeatherKey SERIAL PRIMARY KEY,\n"
                "   AverageTemperature NUMERIC(3,1),\n"
                "   Precipitation NUMERIC(3,1),\n"
                "   Snow NUMERIC(3,1),\n"
                "   WindSpeed NUMERIC(4,1),\n"
                "   Pressure NUMERIC(5,1),\n"
                "   SunTime NUMERIC(5,1)\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS DimRunner(\n"
                "   RunnerKey SERIAL PRIMARY KEY,\n"
                "   Name VARCHAR(100),\n"
                "   Age INT,\n"
                "   Gender VARCHAR(1)\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS DimLocation(\n"
                "   LocationKey SERIAL PRIMARY KEY,\n"
                "   City VARCHAR(50)\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS DimDate(\n"
                "   DateKey SERIAL PRIMARY KEY,\n"
                "   FullDate DATE NOT NULL,\n"
                "   Year INT,\n"
                "   Month INT,\n"
                "   Day INT\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS FactRaceResult(\n"
                "   RaceResultId SERIAL PRIMARY KEY,\n"
                "   DateKey INT REFERENCES DimDate(DateKey),\n"
                "   LocationKey INT REFERENCES DimLocation(LocationKey),\n"
                "   RunnerKey INT REFERENCES DimRunner(RunnerKey),\n"
                "   WeatherKey INT REFERENCES DimWeather(WeatherKey),\n"
                "   OverallRanking INT,\n"
                "   GenderRanking INT,\n"
                "   Time Time,\n"
                "   Pace Time\n"
                ");\n"
            )
            

    def _extract_runners_query(output_folder:str):
        """
        Create a SQL file containing a query to extract all runners from the staging database.
        """
        with open("/opt/airflow/data/extract_runners.sql", "w") as f:
            f.write(
                "SELECT * FROM runners;\n"
            )

    def _extract_weather_query(output_folder:str):
        """
        Create a SQL file containing a query to extract the weather data from the staging database.
        """
        with open("/opt/airflow/data/extract_weather.sql", "w") as f:
            f.write(
                "SELECT weather_id, t_avg, precipitation, snow, wind_speed, pressure, sun FROM weather;\n"
            )

    def _extract_date_query(output_folder:str):
        """
        Create a SQL file containing a query to extract Boston marathons dates from the staging database.
        """
        with open("/opt/airflow/data/extract_date.sql", "w") as f:
            f.write(
                "SELECT marathon_id, full_date, year, month, day FROM marathons ;\n"
            )

    def _extract_location_query(output_folder:str):
        """
        Create a SQL file containing a query to extract the different city of marathons from the staging database.
        """
        with open("/opt/airflow/data/extract_location.sql", "w") as f:
            f.write(
                "SELECT DISTINCT city FROM marathons ;\n"
            )

    def _extract_race_result_query(output_folder:str):
        """
        Create a SQL file containing a query to extract all race results from the staging database.
        """
        with open("/opt/airflow/data/race_result_extract.sql", "w") as f:
            f.write(
                "SELECT \n"
                "   m.marathon_id, \n"
                "   m.city, \n"
                "   run.runner_id, \n"
                "   w.weather_id, \n"
                "   r.ranking, \n"
                "   r.gender_result, \n"
                "   r.time, \n"
                "   r.pace \n"
                "FROM marathons m\n"
                "JOIN weather w ON m.marathon_id = w.marathon_id\n"
                "JOIN result r ON m.marathon_id = r.marathon_id\n"
                "JOIN runners run ON r.runner_id = run.runner_id;\n"
            )

    # Query handlers

    def _runners_query_handler(cursor):
        """
        Export query results about runners data from a database cursor into a CSV file.        
        :param cursor: A database cursor object.
        """
        csv_path = "/opt/airflow/data/runners.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _weather_query_handler(cursor):
        """
        Export query results about weather data from a database cursor into a CSV file.        
        :param cursor: A database cursor object.
        """
        csv_path = "/opt/airflow/data/weather.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _date_query_handler(cursor):
        """
        Export query results about Boston Marathons dates from a database cursor into a CSV file.        
        :param cursor: A database cursor object.
        """
        csv_path = "/opt/airflow/data/date.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _location_query_handler(cursor):
        """
        Export query results about location data from a database cursor into a CSV file.        
        :param cursor: A database cursor object.
        """
        csv_path = "/opt/airflow/data/location.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _race_result_query_handler(cursor):
        """
        Export query results about race results from a database cursor into a CSV file.        
        :param cursor: A database cursor object.
        """
        csv_path = "/opt/airflow/data/race_result.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    # Insert queries

    def _insert_runners_query(output_folder:str):
        """
        Create a SQL file containing a query to insert runners data to the DimRunner of the production pipeline. 
        The runners data comes from the runners CSV file created previously by _runners_query_handler.
        """
        with open("/opt/airflow/data/insert_runners.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/runners.csv")
            f.write(
                "INSERT INTO DimRunner (RunnerKey, Name, Age, Gender)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                runner_id = row.runner_id
                name = row.name
                age = row.age
                gender = row.gender
                values.append(f"({runner_id}, '{name}', '{age}', '{gender}')")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (RunnerKey) DO NOTHING;\n")
            f.write("SELECT setval('dimrunner_runnerkey_seq', COALESCE((SELECT MAX(RunnerKey) FROM DimRunner), 0),  true);\n")

    def _insert_weather_query(output_folder:str):
        """
        Create a SQL file containing a query to insert weather data to the DimWeather of the production pipeline. 
        The weather data comes from the weather CSV file created previously by _weather_query_handler.
        Some weather parameters can be NULL (a few).
        """
        with open("/opt/airflow/data/insert_weather.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/weather.csv")
            f.write(
                "INSERT INTO DimWeather (WeatherKey, AverageTemperature, Precipitation, Snow, WindSpeed, Pressure, SunTime)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                weather_id = row.weather_id
                average_temperature = row.t_avg if pd.notna(row.t_avg) else 'NULL'
                precipitation = row.precipitation if pd.notna(row.precipitation) else 'NULL'
                snow = row.snow if pd.notna(row.snow) else 'NULL'
                wind_speed = row.wind_speed if pd.notna(row.wind_speed) else 'NULL'
                pressure = row.pressure if pd.notna(row.pressure) else 'NULL'
                sun = row.sun if pd.notna(row.sun) else 'NULL'
                values.append(f"({weather_id}, {average_temperature}, {precipitation}, {snow}, {wind_speed}, {pressure}, {sun})")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (WeatherKey) DO NOTHING;\n")
            f.write("SELECT setval('dimweather_weatherkey_seq', COALESCE((SELECT MAX(WeatherKey) FROM DimWeather), 0),  true);\n")

    def _insert_date_query(output_folder:str):
        """
        Create a SQL file containing a query to insert Dates to the DimDate of the production pipeline. 
        The dates comes from the dates CSV file created previously by _date_query_handler.
        """
        with open("/opt/airflow/data/insert_date.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/date.csv")
            f.write(
                "INSERT INTO DimDate (DateKey, FullDate, Year, Month, Day)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                date_key = row.marathon_id
                full_date = row.full_date
                year = row.year
                month = row.month
                day = row.day
                values.append(f"({date_key}, '{full_date}', {year}, {month}, {day})")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (DateKey) DO NOTHING;\n")
            f.write("SELECT setval('dimdate_datekey_seq', COALESCE((SELECT MAX(DateKey) FROM DimDate), 0),  true);\n")

    def _insert_location_query(output_folder:str):
        """
        Create a SQL file containing a query to insert location data to the DimLocation of the production pipeline. 
        The location data comes from the location CSV file created previously by _location_query_handler.
        """
        mapping_city_key = {}
        with open("/opt/airflow/data/insert_location.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/location.csv")
            f.write(
                "INSERT INTO DimLocation (LocationKey, City)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                city = row.city
                if city not in mapping_city_key:
                    mapping_city_key[city] = len(mapping_city_key) + 1
                values.append(f"({mapping_city_key[city]}, '{city}')")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (LocationKey) DO NOTHING;\n")
            f.write("SELECT setval('dimlocation_locationkey_seq', COALESCE((SELECT MAX(LocationKey) FROM DimLocation), 0),  true);\n")
        # As we create the keys for the location entries, we need to store them in order to optimise the insertions for the fact table
        # which reference these keys.
        mapping_df = pd.DataFrame(mapping_city_key.items(), columns=["City", "LocationKey"])
        mapping_df.to_csv(f"/opt/airflow/data/marathon_location_mapping.csv", index=False)

    def _insert_race_result_query(output_folder:str):
        """
        Create a SQL file containing a query to insert race results data to the FactRaceResult of the production pipeline. 
        The race results data comes from the race results CSV file created previously by _race_result_query_handler.
        """
        with open("/opt/airflow/data/insert_race_result.sql", "w") as f:
            df_race_result = pd.read_csv("/opt/airflow/data/race_result.csv")
            # Get location keys stored previously.
            mapping_df = pd.read_csv(f"/opt/airflow/data/marathon_location_mapping.csv")
            mapping_dict = pd.Series(mapping_df.LocationKey.values,index=mapping_df.City).to_dict()

            f.write(
                "INSERT INTO FactRaceResult (DateKey, LocationKey, RunnerKey, WeatherKey, OverallRanking, GenderRanking, Time, Pace)\n"
                "VALUES\n"
            )
            values = []

            for row in df_race_result.itertuples(index=False) :
                datekey = row.marathon_id
                locationkey = mapping_dict.get(row.city, None)
                runnerkey = row.runner_id
                weatherkey = row.weather_id
                overallranking = row.ranking
                genderranking = row.gender_result
                time = row.time
                pace = row.pace
                values.append(f"({datekey}, {locationkey}, {runnerkey}, {weatherkey}, {overallranking}, {genderranking}, '{time}', '{pace}')")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (RaceResultId) DO NOTHING;\n")

    # Operators
    check_db = SQLExecuteQueryOperator(
        task_id="check_db",
        conn_id="postgres_default",
        sql="SELECT 1 FROM pg_database WHERE datname = 'production';",
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
        sql="CREATE DATABASE production;",
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
        conn_id="postgres_production",
        sql="create_tables.sql",
        autocommit=True,
        handler=None,
    )

    extract_runners_query = PythonOperator(
        task_id="extract_runners_query",
        python_callable=_extract_runners_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    extract_weather_query = PythonOperator(
        task_id="extract_weather_query",
        python_callable=_extract_weather_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    extract_date_query = PythonOperator(
        task_id="extract_date_query",
        python_callable=_extract_date_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    extract_location_query = PythonOperator(
        task_id="extract_location_query",
        python_callable=_extract_location_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    extract_race_result_query = PythonOperator(
        task_id="extract_race_result_query",
        python_callable=_extract_race_result_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    extract_runners = SQLExecuteQueryOperator(
        task_id="extract_runners",
        conn_id="postgres_staging",
        sql="extract_runners.sql",
        autocommit=True,
        handler=_runners_query_handler,
    )

    extract_weather = SQLExecuteQueryOperator(
        task_id="extract_weather",
        conn_id="postgres_staging",
        sql="extract_weather.sql",
        autocommit=True,
        handler=_weather_query_handler,
    )

    extract_date = SQLExecuteQueryOperator(
        task_id="extract_date",
        conn_id="postgres_staging",
        sql="extract_date.sql",
        autocommit=True,
        handler=_date_query_handler,
    )

    extract_location = SQLExecuteQueryOperator(
        task_id="extract_location",
        conn_id="postgres_staging",
        sql="extract_location.sql",
        autocommit=True,
        handler=_location_query_handler,
    )

    extract_race_result = SQLExecuteQueryOperator(
        task_id="extract_race_result",
        conn_id="postgres_staging",
        sql="race_result_extract.sql",
        autocommit=True,
        handler=_race_result_query_handler,
    )

    insert_runners_query = PythonOperator(
        task_id="insert_runners_query",
        python_callable=_insert_runners_query,
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

    insert_date_query = PythonOperator(
        task_id="insert_date_query",
        python_callable=_insert_date_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_location_query = PythonOperator(
        task_id="insert_location_query",
        python_callable=_insert_location_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_race_result_query = PythonOperator(
        task_id="insert_race_result_query",
        python_callable=_insert_race_result_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    insert_runners = SQLExecuteQueryOperator(
        task_id="insert_runners",
        conn_id="postgres_production",
        sql="insert_runners.sql",
        autocommit=True,
        handler=None,
    )

    insert_weather = SQLExecuteQueryOperator(
        task_id="insert_weather",
        conn_id="postgres_production",
        sql="insert_weather.sql",
        autocommit=True,
        handler=None,
    )

    insert_date = SQLExecuteQueryOperator(
        task_id="insert_date",
        conn_id="postgres_production",
        sql="insert_date.sql",
        autocommit=True,
        handler=None,
    )

    insert_location = SQLExecuteQueryOperator(
        task_id="insert_location",
        conn_id="postgres_production",
        sql="insert_location.sql",
        autocommit=True,
        handler=None,
    )

    insert_race_result = SQLExecuteQueryOperator(
        task_id="insert_race_result",
        conn_id="postgres_production",
        sql="insert_race_result.sql",
        autocommit=True,
        handler=None,
    )

    join_dimensions = EmptyOperator(
        task_id="join_dimensions",
        trigger_rule="none_failed",
    )

    # DAG
    check_db >> branch >> [create_db, skip_create] >> create_tables_query
    create_tables_query >> create_tables >> [extract_runners_query, extract_weather_query, extract_date_query, extract_location_query]
    extract_runners_query >> extract_runners >> insert_runners_query >> insert_runners
    extract_weather_query >> extract_weather >> insert_weather_query >> insert_weather
    extract_date_query >> extract_date >> insert_date_query >> insert_date
    extract_location_query >> extract_location >> insert_location_query >> insert_location
    [insert_runners, insert_weather, insert_date, insert_location] >> join_dimensions
    join_dimensions >> extract_race_result_query >> extract_race_result >> insert_race_result_query >> insert_race_result
    
