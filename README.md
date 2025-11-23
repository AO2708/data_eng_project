# Marathons X Weather Conditions

<img src="images/logo-insa_0.png" alt="INSA LYON logo" title="Logo of INSA Lyon" width="200" />

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

## Table of Contents

1. [Collaborators](#collaborators)
2. [Abstract](#abstract)
3. [How to Run It](#how-to-run-it)
4. [Notebook Explanation](#notebook-explanation)
5. [How are the Pipelines Designed?](#how-are-the-pipelines-designed-)
   - [Input Datasets](#input-datasets)
   - [Ingestion Pipelines](#ingestion-pipelines)
   - [Staging Pipeline](#staging-pipeline)
   - [Production Pipeline](#production-pipeline)
6. [Queries](#queries)
7. [Requirements](#requirements)

## Collaborators

- Pitard-Bouet Aodren
- Getenet Mathis
- Venaille Arno

## Abstract

## How to run it

1. Download or clone this repository ;
2. With your favorite CLI, open the downloaded/cloned folder ;
3. Run "docker compose up -d" and wait for the process to complete (this may take a few minutes) ;
4. Open your favorite web browser and navigate to "http://localhost:8888/notebooks/project.ipynb" ;
5. You can now execute each code cell in order. Make sure to wait for each cell to finish running before launching the next one.

## Notebook explanation

The idea behind the notebook is to centralize all the steps required to run the project in a single place, so the user doesn't have to create connections in the Airflow UI or manually trigger each pipeline, for example.  
Concretely, the notebook allows the user to :

1. Authenticate and obtain a **jwt token** for subsequent requests to the Airflow API.
2. Create the **necessary connections** between Airflow Server and our Databases (MongoDB, Postgres : default, staging and production).
3. Trigger the **three ingestion pipelines** and wait for their completion.
4. Trigger the **staging pipeline** and wait for its completion.
5. Trigger the **production pipeline** and wait for its completion.
6. Execute the **three analytical requests** and perform **some data science** on the resulting outputs.

## How are the pipelines designed ?

(TODO : Put a schema with the technologies used)

### Input Datasets

**Boston Marathons Race Results : First Dataset**  
We are fetching the Boston Marathons race results from 2015 to 2019 from the github repository https://github.com/adrian3/Boston-Marathon-Data-Project. For each year, a CSV file is retrieved containing the results of each participant.  
The structure of the CSV file is as follows (example showing the header and the first two lines of the 2019 CSV file) :

| place_overall | bib | name              | age | gender | city    | state | country_residence | country_citizenship | name_suffix | 5k   | 10k  | 15k  | 20k  | half | 25k  | 30k  | 35k  | 40k  | pace | projected_time | official_time | overall | gender_result | division_result | seconds | first_name | last_name | display_name     |
| ------------- | --- | ----------------- | --- | ------ | ------- | ----- | ----------------- | ------------------- | ----------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | -------------- | ------------- | ------- | ------------- | --------------- | ------- | ---------- | --------- | ---------------- |
| 1             | 2   | Cherono, Lawrence | 30  | M      | Eldoret | ""    | Kenya             | Kenya               | NULL        | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL           | 2:07:57       | 1       | 1             | 1               | 7677    | NULL       | NULL      | Lawrence Cherono |
| 2             | 6   | Desisa, Lelisa    | 29  | M      | Ambo    | ""    | Ethiopi           | Ethiopi             | NULL        | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL           | 2:07:59       | 2       | 2             | 2               | 7679    | NULL       | NULL      | Lelisa Desisa    |

As you can see, there are missing and NULL values in the data but we will explain the data treatment process in the staging pipeline section.

**Boston Marathon Official Dates : Complementary Dataset**  
As shown in the previous example, the race results data only contains the year of each Boston Marathon edition from 1897 to 2022. However, to match the race results with weather conditions, we need the exact date of each edition. To achieve this, we query Wikidata using SPARQL and extract the response into a CSV file with the following structure (example showing the header and the first two lines) :

| marathon        | edition              | date       | location |
| --------------- | -------------------- | ---------- | -------- |
| Boston Marathon | 1897 Boston Marathon | 1897-04-19 | Boston   |
| Boston Marathon | 1898 Boston Marathon | 1898-04-19 | Boston   |

**Weather Data : Second Dataset**  
We are fetching the weather data for Boston for each day from January 1st, 2015 to December 31st, 2019. To achieve this, we are using the Python _meteostat_ library and storing the response into a JSON file. _Meteostat_ is an open-source Python library that provides easy access to historical weather and climate data from weather stations worldwide, allowing us to retrieve meteorological observations : average temperature (tavg), minimum temperature (tmin), maximum temperature (tmax), precipitation (prcp), snow depth (snow), wind direction (wdir), wind speed (wspd), wind peak gust (wpgt), atmospheric pressure (pres) and sunshine duration (tsun).  
The structure of the JSON file is as follows (partial view shown as the full file contains 1,826 daily records for each parameter) :

```json
{
   "tavg":{"1420070400000": -3.3, "1420156800000": 1.3, ... },
   "tmin": {"1420070400000": -5.5, "1420156800000": -0.5, ...},
   "tmax": {"1420070400000": 0.6, "1420156800000": 5, ...},
   "prcp": {"1420070400000": 0, "1420156800000": 0, ...},
   "snow": {"1420070400000": 0, "1420156800000": 0, ...},
   "wdir": {"1420070400000": null, "1420156800000": null, ...},
   "wspd": {"1420070400000": 23, "1420156800000": 20.5, ...},
   "wpgt": {"1420070400000": null, "1420156800000": null, ...},
   "pres": {"1420070400000": 1016.1, "1420156800000": 1019.8, ...},
   "tsun": {"1420070400000": 544, "1420156800000": 314, ...}
}
```

This structure contains meteorological parameters where:

- the keys are Unix timestamps in milliseconds ;
- the values are measurements in their respective units.

### Ingestion Pipelines

At the end of this step, we will have three MongoDB databases :

- one for Boston Marathon race results from 2000 to 2019 ;
- one for Boston Marathon official dates from 1897 to 2022 ;
- one for the daily weather data of Boston from January 1st, 2015 to December 31st, 2019.

**Ingestion of Boston Marathons data**

<img src="images/ingestion_marathons_airflow_dag.jpg" alt="Airflow Dag of the marathons ingestion" title="Airflow Dag of the marathons ingestion" width="100%" />

The aim of the pipeline above is to ingest 20 CSV files containing Boston Marathon race results from 2000 to 2019.  
The data pipeline consists of 20 parallel tasks. Each task is responsible for ingesting one CSV file (representing data for one edition of the Boston Marathon) and follows a consistent two-step pattern:

1. A BashOperator (get_spreadsheet\_{year}) : Retrieves the CSV file from the GitHub repository (mentioned in the 'Input Datasets' section).
2. A PythonOperator (insert_marathons_data\_{year}) : Inserts the collected raw data into the MongoDB database.

The pipeline can be executed multiple times without inserting duplicates. During insertion, each record is assigned a unique ID by combining the overall ranking and the edition year (example : `1_2019` for the first-place finisher in 2019). A unique index is created on this ID field in the MongoDB database, ensuring that any attempt to insert a duplicate record will be rejected.

At the end of the ingestion, there are `289284` records in the Mongo database.

**Ingestion of Boston Marathons dates**

<img src="images/ingestion_marathons_dates_airflow_dag.jpg" alt="Airflow Dag of the marathons dates ingestion" title="Airflow Dag of the marathons dates ingestion" width="100%" />

The aim of the pipeline above is to ingest the Boston Marathon official dates from 1897 to 2022 (data is not available beyond 2022).  
The data pipeline consists of two sequential tasks :

1. A PythonOperator (extract_wikidata_marathon_date) : Uses the python library `SPARQLWrapper` to query Wikidata for the exact date of each Boston Marathon edition and extracts the results into a CSV file.
2. A PythonOperator (insert_marathons_date_data) : Inserts the collected raw data into the MongoDB database.

The pipeline can be executed multiple times without inserting duplicates. Indeed, a unique index is created on the `edition` field in the MongoDB database, ensuring that any attempt to insert a duplicate record will be rejected.

At the end of the ingestion, there are `126` records in the Mongo database (one for each Boston Marathon from 1897 to 2022).

**Ingestion of Weather Data**

<img src="images/ingestion_weather_airflow_dag.jpg" alt="Airflow Dag of the weather data ingestion" title="Airflow Dag of the weather data ingestion" width="100%" />

The aim of the pipeline above is to ingest the daily weather data of Boston from January 1st, 2015 to December 31st, 2019.  
The data pipeline consists of two sequential tasks :

1. A PythonOperator (run_weather_script) : Uses the python library `meteostat` to query and retrieve historical weather data for Boston.
2. A PythonOperator (insert_weather_data) : Inserts the collected raw data into the MongoDB database.

The pipeline can be executed multiple times without inserting duplicates. During insertion, the JSON document is assigned a unique ID which is the name of the city (Boston here). A unique index is created on this ID field in the MongoDB database, ensuring that any attempt to insert a duplicate record will be rejected.

At the end of the ingestion, there are `1` record in the Mongo database containing `7305` daily records (one for each day from 2000 to 2019) across all meteorological parameters (listed in the 'Input Datasets' section).

### Staging Pipeline

### Production Pipeline

## Queries

## Requirements
