FROM apache/airflow:2.10.2

ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12

USER airflow

# Upgrade installer agar dependency resolution lebih stabil
RUN python -m pip install --upgrade \
    pip \
    setuptools \
    wheel

# Install Python dependencies menggunakan Airflow constraints
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /requirements.txt

# Install dbt pada virtual environment terpisah (untuk Cosmos)
COPY dbt-requirements.txt /dbt-requirements.txt

RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --upgrade pip setuptools wheel && \
    /opt/airflow/dbt_venv/bin/pip install \
    --no-cache-dir \
    -r /dbt-requirements.txt

USER airflow