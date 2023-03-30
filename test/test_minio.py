import unittest
from unittest.mock import patch
from unittest.mock import Mock
from trip.minio import MinioClient
from trip.minio import MinioClientQueryException
from trip.minio import MinioClientConnectionException

class TestMinio(unittest.TestCase):
  @patch('trip.minio.duckdb', Mock())
  def test_query_parquet(self):
    minio_client = MinioClient('endpoint', 'secret_key', 'access_key')
    minio_client.query_parquet('bucket', 'file', 'SELECT * FROM PARQUET')

    parquet = "read_parquet('s3://minio/bucket/file.parquet')"
    minio_client.connection.sql.assert_called_with(f"SELECT * FROM {parquet}")

  @patch('trip.minio.duckdb.connect')
  def test_init_raises_exception(self, mock_duckdb_connection):
    mock_duckdb_connection.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(MinioClientConnectionException):
      minio_client = MinioClient('endpoint', 'secret_key', 'access_key')

  @patch('trip.minio.duckdb', Mock())
  def test_query_parquet_raises_exception(self):
    minio_client = MinioClient('endpoint', 'secret_key', 'access_key')
    minio_client.connection.sql.side_effect = Mock(side_effect = Exception())

    with self.assertRaises(MinioClientQueryException):
      minio_client.query_parquet('bucket', 'file', 'query')

