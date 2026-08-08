from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from staging.tasks.components.extract import Extract
from staging.tasks.components.load import Load


@task_group
def olist_spreadsheet():
  """Task group for Olist Spreadsheet Extract and Load process."""

  extract_task = PythonOperator(
      task_id='extract',
      python_callable=Extract._olist_spreadsheet,
      op_kwargs={'ds': '{{ ds }}'},
      trigger_rule='none_failed',
  )

  load_task = PythonOperator(
      task_id='load',
      python_callable=Load._olist_spreadsheet,
      op_kwargs={'ds': '{{ ds }}'},  
      trigger_rule='none_failed',
  )

  extract_task >> load_task