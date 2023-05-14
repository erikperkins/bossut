from minio import Minio
from datetime import timedelta
import duckdb
import os

DATA = "data"
TAXI = "taxi"
YELLOW_TRIPDATA = "yellow_tripdata_2022"
MINIO_ENDPOINT = "minio.minio.svc.cluster.local:9000"

class DuckDataException(Exception):
  def __init__(self, message):
    super().__init__(message)

class DuckData():
  def __init__(self):
    try:
      self.minio = Minio(
        MINIO_ENDPOINT,
        access_key = os.environ.get('MINIO_ACCESS_KEY'),
        secret_key = os.environ.get('MINIO_SECRET_KEY'),
        secure = False
      )
      self.connection = duckdb.connect(database = ':memory:', read_only = False)
      self.check_data()
    except Exception as e:
      raise DuckDataException(e)

  def check_data(self):
    """Check whether data exists locally. If not, fetch it from MinIO."""
    if not os.path.isdir(DATA):
      os.mkdir(DATA)

    for i in range(1, 13):
      bucket = TAXI
      object = f"{YELLOW_TRIPDATA}-{i:02d}.parquet"
      file = f"{DATA}/{object}"

      if not os.path.exists(file):
        self.minio.fget_object(bucket, object, file)

  def get_trips(self, now):
    """Get trips arriving during the current second."""
    date_parts = {'year': 2022, 'microsecond': 0}
    this_second = now.replace(**date_parts)
    next_second = this_second + timedelta(seconds = 1)

    file = f'yellow_tripdata_2022-{now.month:02d}'
    query = f"""
        SELECT
          strftime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S') as pickup_datetime,
          strftime(tpep_dropoff_datetime, '%Y-%m-%d %H:%M:%S') as dropoff_datetime,
          PULocationID as pickup_location_id,
          DOLocationID as dropoff_location_id,
          passenger_count,
          trip_distance,          
          payment_type,
          fare_amount,
          extra,
          mta_tax,
          tolls_amount,
          improvement_surcharge,
          congestion_surcharge,
          airport_fee,
          tip_amount
        FROM
          PARQUET
        WHERE
          tpep_dropoff_datetime BETWEEN TIMESTAMP '{this_second.strftime('%Y-%m-%d %H:%M:%S')}' 
            AND TIMESTAMP '{next_second.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY
          tpep_dropoff_datetime
        """
    try:
      path = f"{DATA}/{file}"
      return self.query_parquet(path, query)
    except Exception as e:
      raise DuckDataException(f"Failed querying Parquet file: {e}")

  def query_parquet(self, path = None, query = None):
    """
    Query Parquet file in bucket.
    FROM clause should say only PARQUET, to be replaced by the
    specified bucket and object name.
    """
    parquet = f"read_parquet('{path}.parquet')"
    sql = query.replace('PARQUET', parquet)

    try:
      return self.connection.sql(sql).fetch_arrow_table().to_pylist()
    except Exception as e:
      raise DuckDataException(e)
