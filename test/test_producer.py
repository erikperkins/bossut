import unittest
from unittest.mock import patch
from unittest.mock import Mock
from trip.producer import TripProducer
from trip.producer import TripProducerException
from pandas import DataFrame


class TestProducer(unittest.TestCase):
  @patch('trip.producer.KafkaProducer', Mock())
  @patch('trip.producer.DuckData', Mock())
  def setUp(self):
    self.trip_producer = TripProducer()
    self.trip_producer.duckdata.get_trips.return_value = DataFrame({
      'tpep_pickup_datetime': [1, 2],
      'tpep_dropoff_datetime': [3, 4]
    })

  def tearDown(self):
    self.trip_producer.producer.reset_mock()
    self.trip_producer.duckdata.reset_mock()

  def test_send(self):
    self.trip_producer.send()

    self.trip_producer.duckdata.get_trips.assert_called_once()
    self.assertEqual(2, self.trip_producer.producer.send.call_count)

  @patch('trip.producer.KafkaProducer.__new__')
  @patch('trip.producer.DuckData.__new__', Mock())
  def test_init_producer_raises_exception(self, mock_kafka_producer):
    mock_kafka_producer.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(TripProducerException):
      TripProducer()

  @patch('trip.producer.KafkaProducer.__new__', Mock())
  @patch('trip.producer.DuckData.__new__')
  def test_init_duckdata_raises_exception(self, mock_duckdata_producer):
    mock_duckdata_producer.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(TripProducerException):
      TripProducer()

  def test_send_raises_exception(self):
    self.trip_producer.producer.send.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(TripProducerException):
      self.trip_producer.send()
