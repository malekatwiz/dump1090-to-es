import unittest
from unittest.mock import patch, mock_open, MagicMock
import sys
import json
from io import StringIO
from aircraft_json_to_es import index_aircraft_data

class TestAircraftJsonToEs(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data='{"aircraft": [{"hex": "abc123"}]}')
    @patch('aircraft_json_to_es.Elasticsearch')
    @patch('aircraft_json_to_es.bulk')
    def test_index_aircraft_data(self, mock_bulk, mock_es, mock_file):
        # Mock Elasticsearch client
        mock_es_instance = MagicMock()
        mock_es.return_value = mock_es_instance
        mock_es_instance.indices.exists.return_value = False

        # Mock command line arguments
        test_args = ['aircraft_json_to_es.py', 'test_file_path', 'test_index', 'http://localhost:9200', 'test_device']
        with patch.object(sys, 'argv', test_args):
            index_aircraft_data('test_file_path', 'test_index', 'http://localhost:9200', 'test_device')

        # Check if the index creation was called
        mock_es_instance.indices.create.assert_called_once()

        # Check if bulk indexing was called
        mock_bulk.assert_called_once()

    @patch('builtins.open', new_callable=mock_open, read_data='{"aircraft": [{"hex": "abc123"}]}')
    @patch('aircraft_json_to_es.Elasticsearch')
    @patch('aircraft_json_to_es.bulk')
    def test_index_aircraft_data_with_existing_index(self, mock_bulk, mock_es, mock_file):
        # Mock Elasticsearch client
        mock_es_instance = MagicMock()
        mock_es.return_value = mock_es_instance
        mock_es_instance.indices.exists.return_value = True

        # Mock command line arguments
        test_args = ['aircraft_json_to_es.py', 'test_file_path', 'test_index', 'http://localhost:9200', 'test_device']
        with patch.object(sys, 'argv', test_args):
            index_aircraft_data('test_file_path', 'test_index', 'http://localhost:9200', 'test_device')

        # Check if the index creation was not called
        mock_es_instance.indices.create.assert_not_called()

        # Check if bulk indexing was called
        mock_bulk.assert_called_once()

    @patch('builtins.open', new_callable=mock_open, read_data='{"aircraft": [{"hex": "abc123"}]}')
    @patch('aircraft_json_to_es.Elasticsearch')
    @patch('aircraft_json_to_es.bulk', side_effect=Exception("Bulk indexing error"))
    def test_index_aircraft_data_bulk_error(self, mock_bulk, mock_es, mock_file):
        # Mock Elasticsearch client
        mock_es_instance = MagicMock()
        mock_es.return_value = mock_es_instance
        mock_es_instance.indices.exists.return_value = True

        # Mock command line arguments
        test_args = ['aircraft_json_to_es.py', 'test_file_path', 'test_index', 'http://localhost:9200', 'test_device']
        with patch.object(sys, 'argv', test_args):
            with patch('sys.stdout', new=StringIO()) as fake_out:
                with self.assertRaises(SystemExit):
                    index_aircraft_data('test_file_path', 'test_index', 'http://localhost:9200', 'test_device')
                self.assertIn("An error occurred while indexing data: Bulk indexing error", fake_out.getvalue())

if __name__ == '__main__':
    unittest.main()