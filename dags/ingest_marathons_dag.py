import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator

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
    
    def test_mongo_connection(**context):
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['airflow_test']
        collection = db['connection_test']
        collection.insert_one({
            "status": "success",
            "message": "MongoDB connection OK",
        })
        client.close()


    get_spreadsheet = BashOperator(
        task_id="get_spreadsheet",
        bash_command=(
            "curl -fsSL https://raw.githubusercontent.com/ali-ce/datasets/master/Marathon-Majors/Races.csv "
            "--output /opt/airflow/data/major_marathons_data.csv"
        )
    )

    test_mongo = PythonOperator(
        task_id='test_mongo_connection',
        python_callable=test_mongo_connection,
    )

    get_spreadsheet >> test_mongo