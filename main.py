from trip.producer import TripProducer
import sentry_sdk
from time import sleep

sentry_sdk.init(
  dsn = "https://227b9b8e415048598e23fb6764f37db3@sentry.cauchy.link/5",
  traces_sample_rate = 1.0
)

producer = TripProducer()

if __name__ == "__main__":
  while True:
    producer.send()
    sleep(1)
