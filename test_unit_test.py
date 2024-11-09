import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import time
from app import LocalDataStore, KeyExistsError, KeyNotFoundError, KeyTooLongError, ValueTooLargeError, InvalidJSONError, FileSizeLimitExceededError

class TestLocalDataStore(unittest.TestCase):
    def setUp(self):
        self.data_store = LocalDataStore()
        self.data_store.lock = MagicMock()
        self.data_store.data = {}
        self.data_store.file_path = "mocked_file_path.json"
        self.data_store.BATCH_LIMIT = 10
        self.data_store.MAX_VALUE_SIZE = 1024

    @patch("builtins.open", new_callable=mock_open, read_data='{}')
    def test_create_key_successful(self, mock_file):
        with patch('os.path.exists', return_value=True), patch('os.path.getsize', return_value=10):
            result = self.data_store.create("test_key", {"data": "value"}, ttl=10)
            self.assertEqual(result, "Key 'test_key' created successfully.")
            self.assertIn("test_key", self.data_store.data)

    def test_create_key_exists_error(self):
        self.data_store.data["test_key"] = {"value": {"data": "value"}, "expiry": None}
        with self.assertRaises(KeyExistsError):
            self.data_store.create("test_key", {"data": "new_value"})

    def test_create_key_too_long_error(self):
        with self.assertRaises(KeyTooLongError):
            self.data_store.create("a" * 33, {"data": "value"})

    def test_create_value_too_large_error(self):
        with self.assertRaises(ValueTooLargeError):
            self.data_store.create("test_key", {"data": "x" * (self.data_store.MAX_VALUE_SIZE + 1)})

    @patch("builtins.open", new_callable=mock_open)
    def test_read_key_success(self, mock_file):
        self.data_store.data["test_key"] = {"value": {"data": "value"}, "expiry": None}
        result = self.data_store.read("test_key")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["value"], {"data": "value"})

    def test_read_key_not_found_error(self):
        result = self.data_store.read("nonexistent_key")
        self.assertEqual(result["status"], "error")

    def test_read_expired_key(self):
        self.data_store.data["test_key"] = {"value": {"data": "value"}, "expiry": time.time() - 10}
        result = self.data_store.read("test_key")
        self.assertEqual(result["status"], "expired")

    @patch("builtins.open", new_callable=mock_open)
    def test_delete_key_success(self, mock_file):
        self.data_store.data["test_key"] = {"value": {"data": "value"}, "expiry": None}
        result = self.data_store.delete("test_key")
        self.assertEqual(result["status"], "success")
        self.assertNotIn("test_key", self.data_store.data)

    def test_batch_create_limit_exceeded(self):
        keys = {f"key_{i}": {"data": f"value_{i}"} for i in range(self.data_store.BATCH_LIMIT + 1)}
        result = self.data_store.batch_create(keys)
        self.assertEqual(result["status"], "error")

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.getsize")
    def test_enforce_file_size_limit_exceeded(self, mock_getsize, mock_file):
        mock_getsize.return_value = self.data_store.MAX_FILE_SIZE + 1
        with self.assertRaises(FileSizeLimitExceededError):
            self.data_store.create("test_key", {"data": "value"})

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.getsize")
    def test_file_size_limit_enforced(self, mock_getsize, mock_file):
        mock_getsize.return_value = self.data_store.MAX_FILE_SIZE + 1
        with self.assertRaises(FileSizeLimitExceededError):
            self.data_store.save_data()

    @patch("builtins.open", new_callable=mock_open, read_data='{"test_key": {"value": {"data": "value"}, "expiry": null}}')
    def test_load_data(self, mock_file):
        self.data_store.load_data()
        self.assertIn("test_key", self.data_store.data)
        self.assertEqual(self.data_store.data["test_key"]["value"], {"data": "value"})

    @patch("builtins.open", new_callable=mock_open)
    def test_save_data(self, mock_file):
        self.data_store.data["test_key"] = {"value": {"data": "value"}, "expiry": None}
        self.data_store.save_data()
        mock_file().write.assert_called_once_with(json.dumps(self.data_store.data))

    @patch("builtins.open", new_callable=mock_open, read_data='invalid json')
    def test_load_invalid_json(self, mock_file):
        with self.assertRaises(InvalidJSONError):
            self.data_store.load_data()

    def test_is_expired(self):
        past_time = time.time() - 10
        future_time = time.time() + 10
        self.assertTrue(self.data_store.is_expired({"expiry": past_time}))
        self.assertFalse(self.data_store.is_expired({"expiry": future_time}))

if __name__ == "__main__":
    unittest.main()
 
