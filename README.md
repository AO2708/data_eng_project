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
8. [Note for Students](#note-for-students)

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

### Ingestion Pipelines

### Staging Pipeline

### Production Pipeline

## Queries 

## Requirements

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

