import os
from datetime import datetime
from airflow.datasets import Dataset

# Import class yang dibutuhkan dari Cosmos
from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig, ExecutionConfig
from cosmos.constants import ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Relative path ke project dbt Olist
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/warehouse/_dbt"

# 1. Configuration Project dbt
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# 2. Configuration Profile & Connection ke PostgreSQL
profile_config = ProfileConfig(
    profile_name='warehouse',
    target_name='warehouse',
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='staging_db',
        profile_args={'schema': 'warehouse'}
    )
)

# 3. Execution Configuration (Menentukan path biner dbt di venv)
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
)

# 4. Render Configuration (Tanpa dbt_executable_path)
render_config = RenderConfig(
    emit_datasets=True
)

# Definition DbtDag
dag = DbtDag(
    dag_id='olist_warehouse_dbt',
    schedule=[
        Dataset("postgres://warehouse-db:5432/warehouse.staging.orders"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.order_items"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.order_payments"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.order_reviews"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.customers"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.products"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.sellers"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.product_category_name"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.marketing_qualified_leads"),
        Dataset("postgres://warehouse-db:5432/warehouse.staging.closed_deals")
    ],
    catchup=False,
    start_date=datetime(2024, 1, 1),
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config
)