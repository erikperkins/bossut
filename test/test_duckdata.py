import unittest
from unittest.mock import call
from unittest.mock import patch
from unittest.mock import Mock
from trip.duckdata import DuckData
from trip.duckdata import DuckDataException
from datetime import datetime


class TestDuckData(unittest.TestCase):
  @patch('trip.duckdata.Minio', Mock())
  @patch('trip.duckdata.duckdb', Mock())
  @patch('trip.duckdata.os', Mock())
  def setUp(self):
    self.duckdata = DuckData()

    records = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
    self.duckdata.connection.sql.return_value.fetch_arrow_table.return_value.to_pylist.return_value = records

  def tearDown(self):
    # reset mocks
    self.duckdata.minio.reset_mock()
    self.duckdata.connection.reset_mock()

  @patch('trip.duckdata.os')
  def test_check_data_data_absent(self, mock_os):
    mock_os.path.isdir.return_value = False
    mock_os.path.exists.return_value = False

    self.duckdata.check_data()

    mock_os.mkdir.assert_called()

    calls = [
      call(
        "taxi", f"yellow_tripdata_2022-{i:02d}.parquet",
        f"data/yellow_tripdata_2022-{i:02d}.parquet")
      for i in range(1, 13)
    ]
    self.duckdata.minio.fget_object.assert_has_calls(calls)

  @patch('trip.duckdata.os')
  def test_check_data_data_present(self, mock_os):
    mock_os.path.isdir.return_value = True
    mock_os.path.exists.return_value = True

    self.duckdata.check_data()

    mock_os.mkdir.assert_not_called()
    self.duckdata.minio.fget_object.assert_not_called()

  def test_get_trips(self):
    now = datetime.now()
    records = self.duckdata.get_trips(now)
    self.assertEqual([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}], records)

  def test_get_trips_raises_exception(self):
    self.duckdata.query_parquet = Mock()
    self.duckdata.query_parquet.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(DuckDataException):
      self.duckdata.get_trips(datetime.now())

  def test_query_parquet(self):
    records = self.duckdata.query_parquet('file', 'SELECT * FROM PARQUET')

    parquet = "read_parquet('file.parquet')"
    self.duckdata.connection.sql.assert_called_with(f"SELECT * FROM {parquet}")
    self.assertEqual([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}], records)

  @patch('trip.duckdata.Minio.__new__')
  def test_init_minio_raises_exception(self, mock_minio_client):
    mock_minio_client.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(DuckDataException):
      DuckData()

  @patch('trip.duckdata.duckdb.connect')
  def test_init_duckdb_raises_exception(self, mock_duckdb_connection):
    mock_duckdb_connection.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(DuckDataException):
      DuckData()

  @patch('trip.duckdata.os.mkdir')
  @patch('trip.duckdata.DATA', 'test')
  def test_init_check_data_raises_exception(self, mock_os_mkdir):
    mock_os_mkdir.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(DuckDataException):
      DuckData()

  def test_query_parquet_raises_exception(self):
    self.duckdata.connection.sql.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(DuckDataException):
      self.duckdata.query_parquet('file', 'query')