import json
from datetime import timedelta

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import CustomMinio, MinioClient
from pangres import upsert
from sqlalchemy import create_engine

import pandas as pd


class Load:

  @staticmethod
  def _olist_db(table_name, table_pkey, incremental, date):
      bucket_name = 'extracted-data'
      target_date = (pd.to_datetime(date) - timedelta(days=1)).strftime(
          '%Y-%m-%d'
      )

      # Tentukan object_name utama
      if incremental:
        object_name = f'olist-db/{table_name}/data-{target_date}.csv'
      else:
        object_name = f'olist-db/{table_name}/data.csv'

      try:
        # Trik Fallback: Coba ambil data.csv dulu, kalau NoSuchKey coba yang bertanggal
        try:
          df = CustomMinio._get_dataframe(bucket_name, object_name)
        except Exception as e:
          if 'NoSuchKey' in str(e) and not incremental:
            # Fallback jika Variable False tapi file di MinIO ternyata pakai tanggal
            object_name = f'olist-db/{table_name}/data-{target_date}.csv'
            df = CustomMinio._get_dataframe(bucket_name, object_name)
          else:
            raise e

        if df is None or df.empty:
          print(f"{table_name} doesn't have data to load. Skipped...")
          return

        # Formatting primary key jika bertipe list/string
        if isinstance(table_pkey, str) and table_pkey.startswith('['):
          table_pkey = json.loads(table_pkey)

        if table_pkey:
          df = df.set_index(table_pkey)

        engine = create_engine(
            PostgresHook(postgres_conn_id='staging_db').get_uri()
        )
        upsert(
            con=engine,
            df=df,
            table_name=table_name,
            schema='staging',
            if_row_exists='update',
        )
        engine.dispose()

      except Exception as e:
        raise AirflowException(f'Error when loading {table_name}: {str(e)}')
  @staticmethod
  def _olist_db(table_name, table_pkey, incremental, date):
    
    bucket_name = 'extracted-data'
    target_date = (pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')
  
    if incremental:
      object_name = f'olist-db/{table_name}/data-{target_date}.csv'
    else:
      object_name = f'olist-db/{table_name}/data.csv'
  
    try:
      try:
        df = CustomMinio._get_dataframe(bucket_name, object_name)
      except Exception as e:
        if 'NoSuchKey' in str(e) and not incremental:
          object_name = f'olist-db/{table_name}/data-{target_date}.csv'
          df = CustomMinio._get_dataframe(bucket_name, object_name)
        else:
          raise e
  
      if df is None or df.empty:
        print(f"{table_name} doesn't have data to load. Skipped...")
        return
  
      # 1. Parsing jika table_pkey berupa string JSON
      if isinstance(table_pkey, str) and table_pkey.startswith('['):
        table_pkey = json.loads(table_pkey)
  
      # 2. HARD-CHECK: Jika table_pkey kosong/None, beri fallback berdasarkan nama tabel
      if not table_pkey:
        pkey_map = {
            'closed_deals': 'mql_id',
            'marketing_qualified_leads': 'mql_id',
            'customers': 'customer_id',
            'orders': 'order_id',
        }
        table_pkey = pkey_map.get(table_name)
  
      # 3. Set index secara presisi & beri nama index jika belum ada
      if table_pkey:
        if isinstance(table_pkey, str):
          df = df.set_index(table_pkey)
        elif isinstance(table_pkey, list):
          df = df.set_index(table_pkey)
      else:
        # Jika benar-benar tidak ada PK sama sekali di CSV
        raise AirflowException(f'No Primary Key defined for table {table_name}')
  
      # 4. SAFETY NET FOR PANGRES: Pastikan nama index terisi 100%
      if df.index.name is None and not isinstance(df.index, pd.MultiIndex):
        df.index.name = (
            table_pkey if isinstance(table_pkey, str) else table_pkey[0]
        )
  
      engine = create_engine(
          PostgresHook(postgres_conn_id='staging_db').get_uri()
      )
      upsert(
          con=engine,
          df=df,
          table_name=table_name,
          schema='staging',
          if_row_exists='update',
      )
      engine.dispose()
  
    except Exception as e:
      raise AirflowException(f'Error when loading {table_name}: {str(e)}')
  
  @staticmethod
  def _load_supabase_json(object_prefix, target_table, pkey, ds):
    bucket_name = 'extracted-data'
    # FIX: Tambahkan prefix 'olist-api/' pada object_name
    object_name = f'olist-api/{object_prefix}/data-{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'
  
    try:
      engine = create_engine(
          PostgresHook(postgres_conn_id='staging_db').get_uri()
      )
      minio_client = MinioClient._get()
  
      try:
        data_bytes = (
            minio_client.get_object(
                bucket_name=bucket_name, object_name=object_name
            )
            .read()
            .decode('utf-8')
        )
      except Exception as e:
        if 'NoSuchKey' in str(e):
          raise AirflowSkipException(
              f"{object_prefix} doesn't have new data in MinIO ({object_name})."
              ' Skipped...'
          )
        raise e
  
      data = json.loads(data_bytes)
      df = pd.json_normalize(data)
  
      if df is None or df.empty:
        raise AirflowSkipException(
            f"{object_prefix} doesn't have data. Skipped..."
        )
  
      if isinstance(pkey, str):
        if pkey.startswith('['):
          pkey = json.loads(pkey)
        else:
          pkey = [pkey]
  
      if pkey:
        df = df.set_index(pkey)
  
      upsert(
          con=engine,
          df=df,
          table_name=target_table,
          schema='staging',
          if_row_exists='update',
      )
      engine.dispose()
  
    except AirflowSkipException as e:
      raise e
    except Exception as e:
      raise AirflowException(f'Error loading {target_table}: {str(e)}')

  @staticmethod
  def _olist_api_payments(date):
    Load._load_supabase_json(
        object_prefix='order_payments',
        table_name='order_payments',
        table_pkey=['order_id', 'payment_sequential'],  
        date=date,
    )

  @staticmethod
  def _olist_api_payments(ds):
      Load._load_supabase_json(
          'order_payments',
          'order_payments',
          ['order_id', 'payment_sequential'],
          ds,
      )

  @staticmethod
  def _olist_api_reviews(ds):
      Load._load_supabase_json(
          'order_reviews', 'order_reviews', ['review_id', 'order_id'], ds
      )
  @staticmethod
  def _olist_spreadsheet(ds): 
    bucket_name = 'extracted-data'

  
    execution_date = (pd.to_datetime(ds) - timedelta(days=1)).strftime('%Y-%m-%d')
    object_name = f'olist-spreadsheet/product_category/data-{ds}.csv'

    try:
      engine = create_engine(
          PostgresHook(postgres_conn_id='staging_db').get_uri()
      )
      df = CustomMinio._get_dataframe(bucket_name, object_name)

      if df is None or df.empty:
        raise AirflowSkipException(
            "olist_spreadsheet doesn't have data. Skipped..."
        )

      df = df.set_index('product_category_name')

      upsert(
          con=engine,
          df=df,
          table_name='product_category_name_translation',
          schema='staging',
          if_row_exists='update',
      )
      engine.dispose()

    except AirflowSkipException as e:
      raise e
    except Exception as e:
      raise AirflowException(
          f'Error when loading data from olist spreadsheet: {str(e)}'
      )