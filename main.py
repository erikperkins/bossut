from trip.minio import MinioClient
from trip.producer import TripProducer
import sentry_sdk
from time import sleep
import os

sentry_sdk.init(
  dsn = "https://227b9b8e415048598e23fb6764f37db3@sentry.cauchy.link/5",
  traces_sample_rate = 1.0
)

client = MinioClient(
  endpoint = "minio.svc.cluster.local:9000",
  access_key = os.environ.get('MINIO_ACCESS_KEY'),
  secret_key = os.environ.get('MINIO_SECRET_KEY')
)
producer = TripProducer(client)

if __name__ == "__main__":
  while True:
    producer.send()
    sleep(1)
