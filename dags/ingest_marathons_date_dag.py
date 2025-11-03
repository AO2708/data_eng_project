import pendulum
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from pymongo.errors import BulkWriteError
from SPARQLWrapper import SPARQLWrapper, JSON

START_DATE = pendulum.datetime(2025, 10, 20, tz="UTC")

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

query = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT ?marathon ?marathonLabel ?edition ?editionLabel ?date ?locationLabel WHERE {
  VALUES ?marathon {
    wd:Q826038    # Boston Marathon
    wd:Q161222    # Berlin Marathon
    wd:Q578794    # London Marathon
    wd:Q1071822   # Chicago Marathon
    wd:Q752138    # New York City Marathon
    wd:Q1191380   # Tokyo Marathon
    wd:Q15553343  # Sydney Marathon
  }

  ?edition wdt:P31 ?marathon;
           wdt:P585 ?date.
  OPTIONAL { ?edition wdt:P276 ?location. }

  FILTER(YEAR(?date) >= 1897 && YEAR(?date) <= 2014)

  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY ?marathon ?date
"""

with DAG(
    dag_id="ingest_marathons_date",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    template_searchpath=["/opt/airflow/data/"],
    tags=["ingestion"]
) as dag :

    def extract_wikidata_marathon_date(sparql_endpoint, query_str, **context):
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        # --- Parse results ---
        data = []
        for r in results["results"]["bindings"]:
            date_str = r["date"]["value"]
            try:
                iso_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                iso_date = date_str  # fallback

            data.append({
                "marathon": r["marathonLabel"]["value"],
                "edition": r["editionLabel"]["value"],
                "date": iso_date,
                "location": r.get("locationLabel", {}).get("value"),
            })

        df = pd.DataFrame(data)
        df.to_csv("/opt/airflow/data/major_marathons_date_data.csv", index=False)



    def insert_marathons_date_data(**context):
        hook = MongoHook(conn_id='mongo_default')
        client = hook.get_conn()
        db = client['project']
        collection = db['major_marathons_date']
        collection.create_index("edition", unique=True)

        df = pd.read_csv("/opt/airflow/data/major_marathons_date_data.csv")
        data_to_insert = df.to_dict('records')
        try:
            collection.insert_many(data_to_insert, ordered=False)
            print(f"{len(data_to_insert)} inserted documents into 'major_marathons_date' collection.")
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup_count = sum(1 for err in write_errors if err.get("code") == 11000)
            total_inserts = len(data_to_insert) - dup_count
            print(f"{dup_count} detected and ignored duplicates.")
            print(f"{total_inserts} new inserted documents into 'major_marathons_date' collection.")
        
        client.close()

    extract_wikidata_marathon_date = PythonOperator(
        task_id="extract_wikidata_marathon_date",
        python_callable=extract_wikidata_marathon_date,
        op_args=[sparql, query] 
    )

    insert_marathons_date = PythonOperator(
        task_id='insert_marathons_date_data',
        python_callable=insert_marathons_date_data
    )

    extract_wikidata_marathon_date >> insert_marathons_date