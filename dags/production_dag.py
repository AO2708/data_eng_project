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
    
    # Extract queries
    def _create_tables_query(output_folder:str):
        with open("/opt/airflow/data/create_tables.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS DimWeather(\n"
                "   WeatherKey SERIAL PRIMARY KEY,\n"
                "   AverageTemperature NUMERIC(3,1),\n"
                "   Precipitation NUMERIC(3,1),\n"
                "   Snow NUMERIC(3,1),\n"
                "   WindSpeed NUMERIC(4,1),\n"
                "   Pressure NUMERIC(5,1),\n"
                "   SunTime NUMERIC(5,1),\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS DimRunner(\n"
                "   RunnerKey SERIAL PRIMARY KEY,\n"
                "   Name VARCHAR(100),\n"
                "   Age INT,\n"
                "   Genre VARCHAR(1),\n"
                "   Nationality VARCHAR(50)\n"
                ");\n"
            )
            f.write(
                "CREATE TABLE IF NOT EXISTS DimLocalisation(\n"
                "   LocalisationKey SERIAL PRIMARY KEY,\n"
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
                "   RaceResultId SERIAL PRIMARY KEY\n"
                "   DateKey INT REFERENCES DimDate(DateKey),\n"
                "   RaceKey INT REFERENCES DimRace(RaceKey),\n"
                "   RunnerKey INT REFERENCES DimRunner(RunnerKey),\n"
                "   WeatherKey INT REFERENCES DimWeather(WeatherKey),\n"
                "   OverallRanking INT,\n"
                "   GenderRanking INT,\n"
                "   Time Time,\n"
                "   Pace Time\n"
                ");\n"
            )
            

    def _extract_runners_query(output_folder:str):
        with open("/opt/airflow/data/extract_runners.sql", "w") as f:
            f.write(
                "SELECT * FROM runners;\n"
            )

    def _extract_weather_query(output_folder:str):
        with open("/opt/airflow/data/extract_weather.sql", "w") as f:
            f.write(
                "SELECT weather_id, t_avg, precipitation, snow, wind_speed, pressure, sun FROM weather;\n"
            )

    def _extract_date_query(output_folder:str):
        with open("/opt/airflow/data/extract_date.sql", "w") as f:
            f.write(
                "SELECT marathon_id, full_date, year, month, day FROM marathons ;\n"
            )

    # Query handlers

    def _runners_query_handler(cursor):
        csv_path = "/opt/airflow/data/runners.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _weather_query_handler(cursor):
        csv_path = "/opt/airflow/data/weather.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    def _date_query_handler(cursor):
        csv_path = "/opt/airflow/data/date.csv"
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns) 
            writer.writerows(results)

    # Insert queries

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

    def _insert_weather_query(output_folder:str):
        with open("/opt/airflow/data/insert_weather.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/weather.csv")
            f.write(
                "INSERT INTO DimWeather (WeatherKey, AverageTemperature, Precipitation, Snow, WindSpeed, Pressure, Sun)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                weather_id = row.weather_id
                average_temperature = row.t_avg
                precipitation = row.precipitation
                snow = row.snow
                wind_speed = row.wind_speed
                pressure = row.pressure
                sun = row.sun
                values.append(f"({weather_id}, {average_temperature}, {precipitation}, {snow}, {wind_speed}, {pressure}, {sun})")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (WeatherKey) DO NOTHING;\n")
            f.write("SELECT setval('dimweather_weatherkey_seq', COALESCE((SELECT MAX(WeatherKey) FROM DimWeather), 0),  true);\n")

    def _insert_date_query(output_folder:str):
        with open("/opt/airflow/data/insert_date.sql", "w") as f:
            df = pd.read_csv("/opt/airflow/data/date.csv")
            f.write(
                "INSERT INTO DimDate (DateKey, FullDate, Year, Month, Day)\n"
                "VALUES\n"
            )
            values = []
            for row in df.itertuples(index=False) :
                date_key = row.date_key
                full_date = row.full_date
                year = row.year
                month = row.month
                day = row.day
                values.append(f"({date_key}, '{full_date}', {year}, {month}, {day})")
            f.write(", \n".join(values))
            f.write("\nON CONFLICT (DateKey) DO NOTHING;\n")
            f.write("SELECT setval('dimdate_datekey_seq', COALESCE((SELECT MAX(DateKey) FROM DimDate), 0),  true);\n")

    # Operators

    create_tables_query = PythonOperator(
        task_id="create_tables_query",
        python_callable=_create_tables_query,
        op_kwargs={
            "output_folder": "/opt/airflow/data",
        },
    )

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="OLTP",
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

    extract_runners = SQLExecuteQueryOperator(
        task_id="extract_runners",
        conn_id="OLTP",
        sql="extract_runners.sql",
        autocommit=True,
        handler=_runners_query_handler,
    )

    extract_weather = SQLExecuteQueryOperator(
        task_id="extract_weather",
        conn_id="OLTP",
        sql="extract_weather.sql",
        autocommit=True,
        handler=_weather_query_handler,
    )

    extract_date = SQLExecuteQueryOperator(
        task_id="extract_date",
        conn_id="OLTP",
        sql="extract_date.sql",
        autocommit=True,
        handler=_date_query_handler,
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

    insert_runners = SQLExecuteQueryOperator(
        task_id="insert_runners",
        conn_id="OLAP",
        sql="insert_runners.sql",
        autocommit=True,
        handler=None,
    )

    insert_weather = SQLExecuteQueryOperator(
        task_id="insert_weather",
        conn_id="OLAP",
        sql="insert_weather.sql",
        autocommit=True,
        handler=None,
    )

    insert_date = SQLExecuteQueryOperator(
        task_id="insert_date",
        conn_id="OLAP",
        sql="insert_date.sql",
        autocommit=True,
        handler=None,
    )

    insert_weather = SQLExecuteQueryOperator(
        task_id="insert_weather",
        conn_id="OLAP",
        sql="insert_weather.sql",
        autocommit=True,
        handler=None,
    )

    insert_date = SQLExecuteQueryOperator(
        task_id="insert_date",
        conn_id="OLAP",
        sql="insert_date.sql",
        autocommit=True,
        handler=None,
    )

    # DAG

    create_tables_query >> create_tables >> [extract_runners_query, extract_weather_query, extract_date_query]
    extract_runners_query >> extract_runners >> insert_runners_query >> insert_runners
    extract_weather_query >> extract_weather >> insert_weather_query >> insert_weather
    extract_date_query >> extract_date >> insert_date_query >> insert_date
