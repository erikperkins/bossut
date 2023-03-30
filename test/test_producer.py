import unittest
from unittest.mock import patch
from unittest.mock import Mock
from trip.producer import TripProducer
from trip.producer import TripProducerConnectionException
from trip.producer import TripProducerException
from pandas import DataFrame
from datetime import datetime

class TestProducer(unittest.TestCase):
  def setUp(self):
    self.mock_minio_client = Mock()
    self.mock_minio_client.query_parquet.return_value = DataFrame({
      'tpep_pickup_datetime': [1, 2],
      'tpep_dropoff_datetime': [3, 4]
    })

  def tearDown(self):
    self.mock_minio_client.reset_mock()

  @patch('trip.producer.KafkaProducer', Mock())
  def test_get_trips(self):
    trip_producer = TripProducer(self.mock_minio_client)

    trip_producer.get_trips(datetime.now())
    trip_producer.minio.query_parquet.assert_called_once()

  @patch('trip.producer.KafkaProducer', Mock())
  def test_send(self):
    trip_producer = TripProducer(self.mock_minio_client)
    trip_producer.send()

    trip_producer.minio.query_parquet.assert_called_once()
    self.assertEqual(2, trip_producer.producer.send.call_count)

  @patch('trip.producer.KafkaProducer.__new__')
  def test_init_raises_exception(self, mock_kafka_producer):
    mock_kafka_producer.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(TripProducerConnectionException):
      trip_producer = TripProducer(self.mock_minio_client)

  @patch('trip.producer.KafkaProducer', Mock())
  def test_send_raises_exception(self):
    trip_producer = TripProducer(self.mock_minio_client)
    trip_producer.producer.send.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(TripProducerException):
      trip_producer.send()

  @patch('trip.producer.KafkaProducer', Mock())
  def test_get_query_raises_exception(self):
    trip_producer = TripProducer(self.mock_minio_client)
    trip_producer.minio.query_parquet.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(TripProducerException):
      trip_producer.get_trips(datetime.now())