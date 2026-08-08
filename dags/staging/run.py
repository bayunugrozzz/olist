from datetime import datetime
from airflow.decorators import dag
from staging.tasks.olist_db import olist_db
from staging.tasks.olist_api import olist_api
from staging.tasks.olist_spreadsheet import olist_spreadsheet

default_args = {
    "owner": "bayu",
}

@dag(
    dag_id="olist_staging",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["olist"],
    description="Extract, and Load olist data into Staging Area"
)
def olist_staging_dag():
    # Mengalirkan Task Groups
    db_task = olist_db()
    api_task = olist_api()
    spreadsheet_task = olist_spreadsheet()

    [db_task, api_task, spreadsheet_task]

olist_staging_dag()