import json
from airflow.decorators import task_group
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from staging.tasks.components.extract import Extract
from staging.tasks.components.load import Load

@task_group
def olist_db():
    try:
        tables = json.loads(Variable.get('list_olist_table', default_var='[]'))
        table_pkeys = json.loads(Variable.get('pkey_olist_table', default_var='{}'))
        
        # Mapping kolom timestamp untuk tiap tabel (Misal: order_purchase_timestamp, updated_at, dll)
        watermark_cols = json.loads(Variable.get('watermark_olist_table', default_var='{}'))
        
        # Ambil status incremental mode (True/False)
        incremental = Variable.get('olist_staging_incremental_mode', default_var='false').lower() == 'true'
    except Exception:
        tables = []
        table_pkeys = {}
        watermark_cols = {}
        incremental = False

    @task_group
    def extract():
        for table_name in tables:
            # Ambil nama kolom timestamp untuk tabel terkait
            watermark_col = watermark_cols.get(table_name)

            PythonOperator(
                task_id=f"extract_{table_name}",
                python_callable=Extract._olist_db,
                op_kwargs={
                    "table_name": table_name,
                    "incremental": incremental,
                    "watermark_col": watermark_col, 
                    "ds": "{{ ds }}"
                },
                trigger_rule='none_failed'
            )

    @task_group
    def load():
        previous_task = None
        for table_name in tables:
            pkey = table_pkeys.get(table_name, [])
            current_task = PythonOperator(
                task_id=f"load_{table_name}",
                python_callable=Load._olist_db,
                op_kwargs={
                    "table_name": table_name,
                    "table_pkey": pkey,
                    "incremental": incremental,
                    "date": "{{ ds }}"
                },
                trigger_rule='none_failed'
            )

            if previous_task:
                previous_task >> current_task
            previous_task = current_task

    extract() >> load()