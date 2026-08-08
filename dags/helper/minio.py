from io import BytesIO
import json
import os
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from airflow.hooks.base import BaseHook
from minio import Minio
import pandas as pd


class MinioClient:

  @staticmethod
  def _get():
    minio_conn = BaseHook.get_connection('minio')
    raw_endpoint = minio_conn.extra_dejson.get('endpoint_url', 'minio:9000')

    # Pembersihan endpoint URL agar SDK MinIO tidak error
    if '://' in raw_endpoint:
      parsed = urlparse(raw_endpoint)
      endpoint = parsed.netloc or parsed.path
    else:
      endpoint = raw_endpoint

    endpoint = endpoint.rstrip('/')

    return Minio(
        endpoint=endpoint,
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False,
    )


class CustomMinio:

  @staticmethod
  def _ensure_bucket_exists(minio_client, bucket_name):
    if not minio_client.bucket_exists(bucket_name):
      minio_client.make_bucket(bucket_name)

  @staticmethod
  def _put_csv_chunked(
      chunk_generator, bucket_name, object_name, chunksize=50000
  ):
    """Mengekstrak chunk data dan menulisnya ke TEMP FILE DI DISK (bukan RAM), lalu diupload ke MinIO via fput_object.

    Menjamin penggunaan RAM sangat kecil (~beberapa MB saja) & anti Return code
    -9!
    """
    minio_client = MinioClient._get()
    CustomMinio._ensure_bucket_exists(minio_client, bucket_name)

    # Buat file sementara di disk agar RAM tidak terbebani
    with NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as tmp:
      tmp_path = tmp.name
      is_first_chunk = True

      for chunk in chunk_generator:
        # Tulis langsung ke file sementara di disk
        chunk.to_csv(tmp, index=False, header=is_first_chunk)
        is_first_chunk = False

    try:
      # Upload file dari disk langsung ke MinIO
      minio_client.fput_object(
          bucket_name=bucket_name,
          object_name=object_name,
          file_path=tmp_path,
          content_type='text/csv',
      )
    finally:
      # Hapus file sementara di disk setelah upload selesai
      if os.path.exists(tmp_path):
        os.remove(tmp_path)

  @staticmethod
  def _put_csv(df: pd.DataFrame, bucket_name: str, object_name: str):
    """Upload Pandas DataFrame sebagai file CSV ke MinIO."""
    # DIPERBAIKI: Menggunakan MinioClient._get() bukannya CustomMinio._get_client()
    minio_client = MinioClient._get()
    CustomMinio._ensure_bucket_exists(minio_client, bucket_name)

    # Convert DataFrame ke CSV Byte Stream
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='text/csv',
    )

  @staticmethod
  def _put_json(json_data, bucket_name, object_name):
    json_string = json.dumps(json_data)
    json_bytes = json_string.encode('utf-8')

    minio_client = MinioClient._get()
    CustomMinio._ensure_bucket_exists(minio_client, bucket_name)

    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type='application/json',
    )

  @staticmethod
  def _get_dataframe(bucket_name, object_name):
    minio_client = MinioClient._get()
    data = minio_client.get_object(
        bucket_name=bucket_name, object_name=object_name
    )
    return pd.read_csv(data)