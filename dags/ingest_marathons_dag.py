import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    
    get_spreadsheet = BashOperator(
        task_id="get_spreadsheet",
        bash_command=(
            "curl -fsSL https://raw.githubusercontent.com/ali-ce/datasets/master/Marathon-Majors/Races.csv "
            "--output /opt/airflow/data/major_marathons_data.csv"
        )
    )

    get_spreadsheet