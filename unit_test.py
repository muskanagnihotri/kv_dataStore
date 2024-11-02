import unittest
import os
import json
import time
from unittest.mock import patch, mock_open
from app import *

# Assuming LocalDataStore and its dependencies are imported from the module
# from app import LocalDataStore, KeyExistsError, KeyNotFoundError, ValueTooLargeError, KeyTooLongError

class TestLocalDataStore(unittest.TestCase):
    def setUp(self):
        """Set up the test environment before each test."""
        self.test_file_path = 'test_data_store.json'
        self.data_store = LocalDataStore(file_path=self.test_file_path)

    def tearDown(self):
        """Clean up after each test."""
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    def test_create_key(self):
        """Test creating a key."""
        response = self.data_store.create("key1", {"name": "value1"})
        self.assertEqual(response, "Key 'key1' created successfully.")
        self.assertIn("key1", self.data_store.data)
        self.assertEqual(self.data_store.data["key1"]["value"], {"name": "value1"})

    def test_create_existing_key(self):
        """Test creating a key that already exists."""
        self.data_store.create("key1", {"name": "value1"})
        with self.assertRaises(KeyExistsError):
            self.data_store.create("key1", {"name": "value2"})

    def test_create_key_with_long_key(self):
        """Test creating a key that exceeds the maximum length."""
        long_key = "x" * (self.data_store.MAX_KEY_LENGTH + 1)
        with self.assertRaises(KeyTooLongError):
            self.data_store.create(long_key, {"name": "value1"})

    def test_create_value_too_large(self):
        """Test creating a value that exceeds the maximum size."""
        large_value = "x" * (self.data_store.MAX_VALUE_SIZE + 1)
        with self.assertRaises(ValueTooLargeError):
            self.data_store.create("key1", {"data": large_value})

    def test_read_key(self):
        """Test reading a key."""
        self.data_store.create("key1", {"name": "value1"})
        value = self.data_store.read("key1")
        self.assertEqual(value, {"name": "value1"})

    def test_read_nonexistent_key(self):
        """Test reading a key that does not exist."""
        value = self.data_store.read("nonexistent_key")
        self.assertEqual(value, "Error: Key not found or expired.")

    def test_delete_key(self):
        """Test deleting a key."""
        self.data_store.create("key1", {"name": "value1"})
        response = self.data_store.delete("key1")
        self.assertEqual(response, "Key 'key1' deleted successfully.")
        self.assertNotIn("key1", self.data_store.data)

    def test_delete_nonexistent_key(self):
        """Test deleting a key that does not exist."""
        response = self.data_store.delete("nonexistent_key")
        self.assertEqual(response, "Error: Key 'nonexistent_key' not found.")

    def test_ttl_expiration(self):
        """Test key expiration based on TTL."""
        self.data_store.create("key1", {"name": "value1"}, ttl=2)  # TTL of 2 seconds
        time.sleep(3)  # Wait for the key to expire
        value = self.data_store.read("key1")
        self.assertEqual(value, "Error: Key not found or expired.")

    def test_batch_create(self):
        """Test batch creation of keys."""
        response = self.data_store.batch_create({
            "key2": {"name": "value2"},
            "key3": {"name": "value3"}
        })
        self.assertEqual(response, "Batch creation successful.")
        self.assertIn("key2", self.data_store.data)
        self.assertIn("key3", self.data_store.data)

    def test_batch_create_with_errors(self):
        """Test batch creation when one key already exists."""
        self.data_store.create("key2", {"name": "value2"})
        response = self.data_store.batch_create({
            "key2": {"name": "value3"},  # This key already exists
            "key3": {"name": "value3"}
        })
        self.assertIn("key2", response)
        self.assertIn("Error: Key 'key2' already exists.", response)

if __name__ == '_main_':
    unittest.main()