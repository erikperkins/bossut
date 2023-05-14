from kafka import KafkaProducer
from trip.duckdata import DuckData
from datetime import datetime
from pytz import timezone
from json import dumps

BOOTSTRAP_SERVER = "kafka-service.kafka.svc.cluster.local:9092"

class TripProducerException(Exception):
  def __init__(self, message):
    super().__init__(message)


class TripProducer:
  """Read taxi cab trips from MinIO and publish to Kafka."""
  def __init__(self):
    try:
      self.duckdata = DuckData()
    except Exception as e:
      raise TripProducerException(f"Failed creating DuckDB: {e}")

    try:
      self.producer = KafkaProducer(
        bootstrap_servers = [BOOTSTRAP_SERVER],
        value_serializer = lambda x: dumps(x).encode('utf-8')
      )
    except Exception as e:
      raise TripProducerException(f"Failed creating Kafka producer: {e}")


  def send(self):
    """
    Query current minute of trips from parquet file in MinIO.
    Publish trip events to Kafka.
    """
    now = datetime.now(timezone('US/Eastern'))
    records = self.duckdata.get_trips(now)

    for record in records:
      try:
        self.producer.send('trips', record)
      except Exception as e:
        raise TripProducerException(f"Failed sending message to Kafka: {e}")

