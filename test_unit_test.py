import unittest
from unittest.mock import patch, mock_open, MagicMock
import json
import time
from app import LocalDataStore, KeyExistsError, KeyNotFoundError, KeyTooLongError, ValueTooLargeError, InvalidJSONError, FileSizeLimitExceededError

class TestLocalDataStore(unittest.TestCase):
    def setUp(self):
        # Instantiate LocalDataStore and set required attributes for testing
        self.data_store = LocalDataStore()
        self.data_store.lock = MagicMock()  # Mocking the lock to avoid thread locking issues in tests
        self.data_store.data = {}  # Initializing data as an empty dictionary
        self.data_store.file_path = "mocked_file_path.json"  # Mocked file path
        self.data_store.BATCH_LIMIT = 10  # Setting a batch limit for testing
        self.data_store.MAX_VALUE_SIZE = 1024  # Define max value size for testing

    @patch("builtins.open", new_callable=mock_open, read_data='{}')
    def test_create_key_successful(self, mock_file):
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
        self.assertEqual(result["status"], "failed")

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

    def test_delete_key_not_found_error(self):
        result = self.data_store.delete("nonexistent_key")
        self.assertEqual(result["status"], "failed")

    def test_is_key_expired(self):
        self.data_store.data["test_key"] = {"value": {"data": "value"}, "expiry": time.time() - 10}
        self.assertTrue(self.data_store.is_key_expired("test_key"))

    def test_cleanup_expired_keys(self):
        self.data_store.data["expired_key"] = {"value": {"data": "expired"}, "expiry": time.time() - 10}
        self.data_store.data["valid_key"] = {"value": {"data": "valid"}, "expiry": time.time() + 10}
        self.data_store.cleanup_expired_keys()
        self.assertNotIn("expired_key", self.data_store.data)
        self.assertIn("valid_key", self.data_store.data)

    @patch("builtins.open", new_callable=mock_open)
    def test_batch_create_successful(self, mock_file):
        kv_pairs = {"key1": {"data": "value1"}, "key2": {"data": "value2"}}
        result = self.data_store.batch_create(kv_pairs)
        self.assertEqual(result["status"], "success")
        self.assertIn("key1", self.data_store.data)
        self.assertIn("key2", self.data_store.data)

    def test_batch_create_limit_exceeded(self):
        kv_pairs = {f"key{i}": {"data": "value"} for i in range(self.data_store.BATCH_LIMIT + 1)}
        result = self.data_store.batch_create(kv_pairs)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["message"], f"Batch limit exceeded. Maximum {self.data_store.BATCH_LIMIT} pairs allowed.")

    @patch("builtins.open", new_callable=mock_open)
    def test_invalid_json_error_handling(self, mock_file):
        mock_file.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        with self.assertRaises(InvalidJSONError):
            self.data_store.load_data()

    @patch("builtins.open", new_callable=mock_open)
    def test_enforce_file_size_limit_exceeded(self, mock_file):
        self.data_store.data = {f"key{i}": {"value": "x" * self.data_store.MAX_VALUE_SIZE, "expiry": None} for i in range(1000)}
        with self.assertRaises(FileSizeLimitExceededError):
            self.data_store.enforce_file_size_limit()

if __name__ == '_main_':
    unittest.main()


