from kafka import KafkaProducer
from datetime import datetime
from datetime import timedelta
from json import dumps


class TripProducerConnectionException(Exception):
  def __init__(self, message):
    super().__init__(message)


class TripProducerException(Exception):
  def __init__(self, message):
    super().__init__(message)


class TripProducer:
  """Read taxi cab trips from MinIO and publish to Kafka."""
  def __init__(self, client):
    self.minio = client
    try:
      self.producer = KafkaProducer(
        bootstrap_servers = ['kafka-service.kafka.svc.cluster.local:9092'],
        value_serializer = lambda x: dumps(x).encode('utf-8')
      )
    except Exception as e:
      raise TripProducerConnectionException(f"Failed creating Kafka producer: {e}")

  def send(self):
    """
    Query current minute of trips from parquet file in MinIO.
    Publish trip events to Kafka.
    """
    now = datetime.now()
    results = self.get_trips(now)

    columns = {'tpep_pickup_datetime': 'str', 'tpep_dropoff_datetime': 'str'}
    records = results.astype(columns).to_dict(orient = 'records')

    for record in records:
      try:
        self.producer.send('trips', record)
      except Exception as e:
        raise TripProducerException(f"Failed sending message to Kafka: {e}")

  def get_trips(self, now):
    """Get trips arriving during the current second."""
    date_parts = {'year': 2022, 'microsecond': 0}
    this_second = now.replace(**date_parts)
    next_second = this_second + timedelta(seconds = 1)

    bucket = 'taxi'
    file = f'yellow_tripdata_2022-{now.month:02d}'
    query = f"""
      SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        trip_distance,
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
      return self.minio.query_parquet(bucket, file, query)
    except Exception as e:
      raise TripProducerException(f"Failed querying Parquet file: {e}")


