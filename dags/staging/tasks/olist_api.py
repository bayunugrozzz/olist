from airflow.decorators import task_group
from airflow.operators.python import PythonOperator

from staging.tasks.components.extract import Extract
from staging.tasks.components.load import Load

@task_group()
def olist_api():
    """
    Task group for Olist API (Payments & Reviews) Extract and Load process.
    """

    extract_payments = PythonOperator(
        task_id='extract_order_payments',
        python_callable=Extract._olist_api_payments,
        trigger_rule='none_failed'
    )

    load_payments = PythonOperator(
        task_id='load_order_payments',
        python_callable=Load._olist_api_payments,
        trigger_rule='none_failed'
    )

    extract_payments >> load_payments

    extract_reviews = PythonOperator(
        task_id='extract_order_reviews',
        python_callable=Extract._olist_api_reviews,
        trigger_rule='none_failed'
    )

    load_reviews = PythonOperator(
        task_id='load_order_reviews',
        python_callable=Load._olist_api_reviews,
        trigger_rule='none_failed'
    )

    extract_reviews >> load_reviews