# Olist E-Commerce End-to-End Modern Data Platform

An **enterprise-grade, scalable Data Platform** engineered to ingest, store, transform, and model multi-source e-commerce data from the **Olist Brazilian E-Commerce Dataset**.

The platform combines a **Dynamic Incremental Extraction Engine**, high-performance ingestion with **Polars & ConnectorX**, object storage through **MinIO**, and automated **dbt Dimensional Modeling** orchestrated by **Apache Airflow & Astronomer Cosmos**.

> **Goal:** Transform fragmented operational data into a reliable, tested, and analytics-ready data warehouse.

---

## Executive Summary

This project demonstrates an end-to-end modern data engineering architecture designed around several core principles:

- **Incremental ingestion** instead of expensive full-table extraction.
- **Late-arriving data handling** using a configurable lookback window.
- **High-performance extraction** using Polars and ConnectorX.
- **Idempotent loading** using PostgreSQL UPSERT patterns.
- **Raw data preservation** in an S3-compatible data lake.
- **Dimensional modeling** using dbt and a Star Schema.
- **Automated data quality testing** for analytical reliability.
- **Containerized infrastructure** for reproducible local development.

The extraction layer is designed to reduce unnecessary source database I/O by up to **90%** when compared with repeated full-table extraction, depending on source-table growth and workload characteristics.

---

# Architecture & Data Flow

The platform is organized into five major layers:

```text
┌─────────────────────┐
│   SOURCE SYSTEMS    │
├─────────────────────┤
│ PostgreSQL          │
│ Supabase REST API   │
│ Google Sheets API   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────┐
│   INGESTION & ORCHESTRATION │
├─────────────────────────────┤
│ Apache Airflow              │
│ Polars                      │
│ ConnectorX                  │
│ Dynamic Incremental Window  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────┐
│    DATA LAKE        │
├─────────────────────┤
│ MinIO / S3          │
│ Raw CSV / JSON      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────┐
│      DATA WAREHOUSE         │
├─────────────────────────────┤
│ PostgreSQL                  │
│ Staging                     │
│ UPSERT / Idempotent Loading │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│     TRANSFORMATION          │
├─────────────────────────────┤
│ dbt Core                    │
│ Staging Models              │
│ Incremental Models          │
│ Snapshots / SCD Type 2      │
│ Data Quality Tests          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│       DATA MARTS            │
├─────────────────────────────┤
│ Star Schema                 │
│ Fact Tables                 │
│ Dimension Tables             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────┐
│   BI / ANALYTICS    │
│ Metabase / Looker   │
└─────────────────────┘
```

---

## 1. Source Systems

The platform integrates data from multiple upstream sources.

### PostgreSQL — Transactional Database

Contains core e-commerce transactional entities such as:

- `orders`
- `order_items`
- `customers`
- `products`
- `sellers`

### Supabase REST API

Provides additional datasets such as:

- `order_reviews`
- `order_payments`

### Google Sheets API

Provides external business enrichment data, including product-category mappings.

---

## 2. Ingestion & Extraction Layer

Data ingestion and orchestration are managed by **Apache Airflow**.

The extraction engine uses:

- **Polars** for high-performance dataframe processing.
- **ConnectorX** for efficient database-to-dataframe transfers.
- **Dynamic Incremental Extraction** to avoid unnecessary full-table scans.
- **Airflow logical execution dates** to determine extraction windows.

### Dynamic Incremental Window

The pipeline applies a configurable lookback strategy to capture late-arriving records.

Conceptually:

```text
Previous Window
      │
      ▼
┌─────────────────────────────────────────────┐
│ H-2 │ H-1 │ H │ H+1                         │
└─────────────────────────────────────────────┘
      ▲
      │
  Lookback
```

The lookback window helps protect the pipeline from upstream records that arrive later than their expected processing period.

Timestamp-based filtering is applied to appropriate source columns, for example:

- `order_purchase_timestamp`
- `review_creation_date`

Where applicable, source tables should be indexed on these watermark columns to support efficient range scans.

---

## 3. Landing Zone — MinIO

Extracted source data is persisted into **MinIO**, an S3-compatible object storage layer.

The landing zone acts as a raw, durable staging area.

Typical formats include:

```text
CSV
JSON
```

Conceptually:

```text
Source
  │
  ▼
Airflow
  │
  ▼
Polars / ConnectorX
  │
  ▼
MinIO
  │
  ├── raw/
  ├── postgres/
  ├── api/
  └── spreadsheet/
```

Keeping raw data separate from transformed warehouse data provides an additional layer of reproducibility and recovery.

---

# 4. Data Warehouse & Transformation

Raw data from MinIO is loaded into **PostgreSQL** staging tables using an idempotent loading strategy.

The loading process uses PostgreSQL-native UPSERT semantics:

```sql
INSERT INTO target (...)
VALUES (...)
ON CONFLICT (business_key)
DO UPDATE SET
    ...
;
```

This approach helps prevent duplicate records while allowing the pipeline to safely process:

- retries
- backfills
- overlapping incremental windows
- late-arriving records

---

## dbt Transformation Layer

**dbt Core** is responsible for transformation, modeling, testing, and analytical data structure.

The transformation layer is organized into:

```text
Raw / Staging
      │
      ▼
dbt Staging Models
      │
      ▼
Business Transformations
      │
      ▼
Dimensional Models
      │
      ├── Dimensions
      └── Facts
      │
      ▼
Analytics / BI
```

### dbt capabilities used

- Staging models
- Incremental models
- Snapshots
- Seeds
- Macros
- Data tests
- Dimensional modeling

---

# 5. Production Data Marts

The final analytical layer follows a **Star Schema** architecture.

Example:

```text
                   ┌───────────────┐
                   │ dim_customers │
                   └───────┬───────┘
                           │
                           │
┌──────────────┐     ┌─────▼──────┐     ┌─────────────┐
│ dim_products │────►│ fact_sales │◄────│ dim_sellers │
└──────────────┘     └─────┬──────┘     └─────────────┘
                           │
                           ▼
                    BI / Analytics
```

The marts are designed to be consumed by analytical tools such as **Metabase** or **Looker Studio**.

---

# 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10+ |
| Workflow Integration | Astronomer Cosmos |
| Language | Python 3.12 |
| High-Performance Processing | Polars |
| Database Extraction | ConnectorX |
| Data Lake | MinIO |
| Data Warehouse | PostgreSQL |
| Transformation | dbt Core 1.12+ |
| Source API | Supabase REST API |
| Spreadsheet Integration | Google Sheets API / `gspread` |
| Containerization | Docker / Docker Compose |

---

# Key Engineering Strategies

## 1. Dynamic Incremental Extraction

Instead of repeatedly extracting entire source tables, the pipeline determines a dynamic extraction boundary based on the Airflow execution date.

```text
Full Extraction

Source ───────────────────────────────► Everything
        ↑
        Expensive I/O


Incremental Extraction

Source ────────┬──────────────────────► Recent Window
               │
               └── H-2 → H+1
```

Benefits:

- Lower source DB I/O
- Faster pipeline execution
- Lower network traffic
- Better scalability
- Support for late-arriving data

---

## 2. High-Throughput Ingestion with Polars

The ingestion layer uses **Polars** instead of conventional Pandas-based extraction for workloads where high-performance dataframe processing is beneficial.

Combined with ConnectorX, this enables efficient database-to-dataframe transfers while reducing unnecessary intermediate processing.

---

## 3. Idempotent Warehouse Loading

The warehouse loading process is designed to be safely rerunnable.

```text
Raw Data
   │
   ▼
MinIO
   │
   ▼
UPSERT
   │
   ├── New record ─────► INSERT
   │
   └── Existing record ► UPDATE
```

This makes retries and backfills safer because processing the same source window does not inherently create duplicate records.

---

## 4. Isolated dbt Schema Management

dbt models are organized into controlled warehouse schemas to avoid unwanted schema proliferation and maintain a predictable analytical environment.

A custom `generate_schema_name` macro can be used to enforce a unified production schema.

Example:

```text
PostgreSQL
└── warehouse
    ├── staging
    ├── dimensions
    └── facts
```

---

## 5. Automated Data Quality

Data quality is treated as part of the transformation pipeline rather than a separate manual process.

The project uses dbt tests for:

### Uniqueness

Ensures business and surrogate keys remain unique.

```text
order_id
customer_id
seller_id
product_id
```

### Not Null

Critical analytical attributes are validated against unexpected NULL values.

Examples:

```text
price
freight_value
order_id
customer_id
```

### Referential Integrity

Relationships between fact and dimension tables are validated.

```text
fact_sales
    │
    ├──► dim_customers
    ├──► dim_products
    └──► dim_sellers
```

---

# Core Data Marts

| Mart Table | Model Type | Business Description |
|---|---|---|
| `fact_sales` | Fact | Transaction items, pricing, freight fees, and customer/seller relationships |
| `fact_marketing_funnel` | Fact | Marketing funnel, lead assignment, closed deals, and sales conversion metrics |
| `dim_sellers` | Dimension | Seller master data, geographic attributes, and operational information |
| `dim_customers` | Dimension | Unique customer entities and geographic attributes |
| `dim_products` | Dimension | Product catalog enriched with translated category information |

---

# Repository Structure

```text
.
├── dags/
│   ├── olist_warehouse_dbt.py
│   └── staging/
│       ├── olist_db.py
│       ├── olist_api.py
│       └── olist_spreadsheet.py
│
├── staging/
│   └── tasks/
│       └── components/
│           ├── extract.py
│           └── load.py
│
├── warehouse/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── snapshots/
│   ├── tests/
│   └── dbt_project.yml
│
├── helper/
│   └── minio.py
│
├── docker-compose.yml
├── Dockerfile
├── dbt-requirements.txt
├── requirements.txt
├── start.sh
└── README.md
```

> **Note:** Environment files, local databases, notebooks, checkpoints, credentials, and other development artifacts should remain excluded through `.gitignore`.

---

# Getting Started

## Prerequisites

Make sure the following are installed:

- Docker
- Docker Compose
- Git
- Python 3.10+

---

## 1. Clone the Repository

```bash
git clone https://github.com/bayunugz/olist-data-pipeline.git
cd olist-data-pipeline
```

---

## 2. Configure Environment Variables

Create a `.env` file in the project root.

Example:

```env
AIRFLOW_UID=50000

POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

SUPABASE_API_KEY=your_supabase_key
```

> **Important:** Never commit `.env` files, API keys, passwords, service-account credentials, or other secrets to Git.

---

## 3. Start the Infrastructure

Start the containerized infrastructure:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

---

# 🔌 Airflow Connections

After starting Airflow, configure the required connections through the Airflow UI.

Default URL:

```text
http://localhost:8080
```

Expected connections include:

| Connection ID | Purpose |
|---|---|
| `olist_db` | PostgreSQL source database |
| `staging_db` | PostgreSQL warehouse/staging database |
| `olist_analytics` | Google Sheets integration |

Additional connections may be required depending on the deployment configuration.

---

# Data Quality & Testing

Run dbt tests manually from the dbt project directory:

```bash
cd warehouse
dbt test
```

To test only the marts:

```bash
dbt test --select marts
```

To run and test models:

```bash
dbt build
```

A successful pipeline should satisfy the configured:

- uniqueness tests
- not-null tests
- relationship tests
- custom business rules

---

# 🔄 End-to-End Pipeline

A typical execution follows this sequence:

```text
                ┌──────────────────┐
                │  Source Systems  │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │     Airflow      │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │ Polars +         │
                │ ConnectorX       │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │      MinIO       │
                │    Raw Layer     │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │    PostgreSQL    │
                │ Staging / UPSERT │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │      dbt         │
                │ Transformations  │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │   Star Schema    │
                │   Data Marts     │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │ BI / Analytics   │
                └──────────────────┘
```

---

# Business & Engineering Impact

This project demonstrates how raw, fragmented e-commerce data can be transformed into a structured analytical platform through a modern data engineering architecture.

### Engineering Impact

- **Incremental processing** reduces unnecessary source-system workload.
- **Lookback windows** improve reliability against late-arriving data.
- **Polars + ConnectorX** provide a high-performance ingestion path.
- **MinIO** provides a durable raw-data landing layer.
- **UPSERT-based loading** supports idempotent pipeline execution.
- **dbt** provides modular transformation and analytical modeling.
- **Automated testing** increases trust in downstream datasets.
- **Docker Compose** enables reproducible local infrastructure.

### Business Impact

The final warehouse provides a consistent analytical foundation for understanding:

- Sales performance
- Customer behavior
- Seller performance
- Product performance
- Freight and transaction economics
- Marketing funnel conversion

---

# uture Improvements

Potential extensions to the platform include:

- [ ] Add CI/CD for dbt validation
- [ ] Add automated deployment workflows
- [ ] Add data observability
- [ ] Add pipeline SLA monitoring
- [ ] Add Great Expectations or additional data-quality frameworks
- [ ] Add Kafka for event-driven ingestion
- [ ] Add Spark for larger-scale processing workloads
- [ ] Add dedicated BI dashboards
- [ ] Add infrastructure monitoring with Prometheus/Grafana
- [ ] Add automated lineage documentation
- [ ] Add production cloud deployment

---

# Project

**Olist E-Commerce End-to-End Modern Data Platform**

Built as a portfolio-grade modern data engineering project demonstrating:

```text
Ingestion
   ↓
Orchestration
   ↓
Data Lake
   ↓
Data Warehouse
   ↓
Transformation
   ↓
Dimensional Modeling
   ↓
Data Quality
   ↓
Analytics
```

---

## Why This Project?

The goal is not simply to move data from point A to point B.

It is to demonstrate how a production-oriented data platform can be designed around:

> **Performance · Reliability · Scalability · Reproducibility · Data Quality**
