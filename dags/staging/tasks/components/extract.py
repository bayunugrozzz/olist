from datetime import timedelta
import json
from io import BytesIO, StringIO
import requests
import pandas as pd
import polars as pl
import gspread

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.minio import CustomMinio


class Extract:

  @staticmethod
  def _olist_db(table_name, ds, incremental=False, watermark_col=None, lookback_days=2):
   """
   Ekstraksi PostgreSQL dengan dukungan Dynamic Incremental Window.
   - jika incremental=False: FULL EXTRACT
   - jika incremental=True: INCREMENTAL EXTRACT berdasarkan watermark_col & ds
   """
   try:
       pg_hook = PostgresHook(postgres_conn_id='olist_db')
       conn_uri = pg_hook.get_uri()
       current_ds = pd.to_datetime(ds)
       start_date = (current_ds - timedelta(days=lookback_days)).strftime('%Y-%m-%d 00:00:00')
       end_date = (current_ds + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
      
       if incremental and watermark_col:
           query = f"""
               SELECT * 
               FROM raw.{table_name}
               WHERE {watermark_col} >= '{start_date}' 
                 AND {watermark_col} < '{end_date}'
           """
           print(f"Executing INCREMENTAL extract for {table_name} using column '{watermark_col}' range [{start_date} to {end_date}]")
       else:
           query = f"SELECT * FROM raw.{table_name}"
           print(f"Executing FULL extract for {table_name}")

       df = pl.read_database_uri(query, uri=conn_uri)
       if df.is_empty():
           print(f"No new records found for {table_name} on {ds}.")
           return
       
       execution_date = (current_ds - timedelta(days=1)).strftime('%Y-%m-%d')
       object_name = f'olist-db/{table_name}/data-{execution_date}.csv'

       CustomMinio._put_csv(df.to_pandas(), 'extracted-data', object_name)
       print(f"Successfully extracted {len(df)} records for {table_name}")
       
   except Exception as e:
       raise AirflowException(f'Error when extracting {table_name}: {str(e)}')

  @staticmethod
  def _fetch_supabase_api(api_url_var, object_prefix, ds):
    """Helper generic untuk menarik data API Supabase menggunakan Publishable Key."""
    try:
      # Ambil URL & Key dari Airflow Variable
      api_url = Variable.get(api_url_var)
      api_key = Variable.get('supabase_api_key').strip().strip('"\'')

      # DIESUAIKAN UNTUK CARA 2:
      # Cukup gunakan 'apikey', HAPUS 'Authorization: Bearer ...'
      headers = {
          'apikey': api_key,
          'Accept': 'application/json',
          'Content-Type': 'application/json',
      }

      params = {'select': '*'}

      response = requests.get(
          url=api_url, headers=headers, params=params, timeout=30
      )

      if response.status_code != 200:
        raise AirflowException(
            f'Failed to fetch {object_prefix}. Status: {response.status_code},'
            f' Msg: {response.text}'
        )

      json_data = response.json()
      if not json_data:
        raise AirflowSkipException(
            f'No data found for {object_prefix}. Skipped...'
        )

      # Normalisasi karakter newline di dalam string
      def replace_newlines(obj):
        if isinstance(obj, dict):
          return {k: replace_newlines(v) for k, v in obj.items()}
        elif isinstance(obj, list):
          return [replace_newlines(elem) for elem in obj]
        elif isinstance(obj, str):
          return obj.replace('\n', ' ')
        return obj

      json_data = replace_newlines(json_data)

      execution_date = (pd.to_datetime(ds) - timedelta(days=1)).strftime(
          '%Y-%m-%d'
      )
      bucket_name = 'extracted-data'
      object_name = f'olist-api/{object_prefix}/data-{execution_date}.json'

      CustomMinio._put_json(json_data, bucket_name, object_name)
      print(
          f'Successfully extracted {len(json_data)} records for {object_prefix}'
      )

    except AirflowSkipException as e:
      raise e
    except Exception as e:
      raise AirflowException(f'Error extracting {object_prefix}: {str(e)}')
  @staticmethod
  def _olist_api_payments(ds):
    Extract._fetch_supabase_api('olist_api_payments_url', 'order_payments', ds)

  @staticmethod
  def _olist_api_reviews(ds):
    Extract._fetch_supabase_api('olist_api_reviews_url', 'order_reviews', ds)

  @staticmethod
  def _olist_spreadsheet(ds):
    """Mengekstrak Google Spreadsheet menggunakan gspread + Credentials dari Connection 'olist_analytics'."""
    try:
      gcp_hook = GoogleBaseHook(gcp_conn_id='olist_analytics')
      credentials = gcp_hook.get_credentials()

      gc = gspread.authorize(credentials)

      spreadsheet_id = '1qb5s_wdw0-2JlFmy08R-P5xnyAMBjEnb1sxzVGm6rnI'
      sh = gc.open_by_key(spreadsheet_id)

      worksheet = sh.get_worksheet(0)

      data = worksheet.get_all_records()
      df = pd.DataFrame(data)

      if df.empty:
        print('Spreadsheet is empty.')
        return

      bucket_name = 'extracted-data'
      object_name = f'olist-spreadsheet/product_category/data-{ds}.csv'

      CustomMinio._put_csv(df, bucket_name, object_name)
      print(
          f'Successfully extracted {len(df)} rows from Spreadsheet to MinIO'
          ' using gspread!'
      )

    except Exception as e:
      raise AirflowException(f'Error extracting spreadsheet with gspread: {e}')