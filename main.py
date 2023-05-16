from trip.producer import TripProducer
import sentry_sdk
from time import sleep

sentry_sdk.init(
  dsn = "https://d2ac4e11398b4f1ba34ae288fd866874@sentry.cauchy.link/3",
  traces_sample_rate = 1.0
)

producer = TripProducer()

if __name__ == "__main__":
  while True:
    producer.send()
    sleep(1)
