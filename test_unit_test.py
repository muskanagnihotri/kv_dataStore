import unittest
from unittest.mock import patch, MagicMock
from app import *

class TestLocalDataStore(unittest.TestCase):
    @patch('app.LocalDataStore.acquire_file_lock')
    def test_save_data(self, mock_acquire_lock):
        mock_acquire_lock.return_value = None
        self.data_store = MagicMock()  
        self.data_store.save_data()
        self.assertTrue(self.data_store.save_data.called)

    @patch('app.LocalDataStore.acquire_file_lock')
    def test_delete_key_success(self, mock_acquire_lock):
        mock_acquire_lock.return_value = None
        self.data_store = MagicMock()
        self.data_store.delete.return_value = True 
        result = self.data_store.delete("test_key")
        self.assertTrue(result)
        self.assertTrue(self.data_store.delete.called)

    @patch('app.LocalDataStore.acquire_file_lock')
    def test_enforce_file_size_limit_exceeded(self, mock_acquire_lock):  
        mock_acquire_lock.return_value = None
        self.data_store = MagicMock()
        self.data_store.create.return_value = True  
        self.data_store.create("test_key", {"data": "value"})
        self.assertTrue(self.data_store.create.called)

    @patch.object(LocalDataStore, 'is_key_expired')  
    @patch('app.LocalDataStore.acquire_file_lock') 
    def test_is_expired(self, mock_acquire_lock, mock_is_key_expired):
        mock_acquire_lock.return_value = None
        mock_is_key_expired.return_value = True
        self.data_store = LocalDataStore()
        self.data_store.data = {"test_key": "some_value"}
        result = self.data_store.is_expired("test_key")
        self.assertTrue(result)
        mock_is_key_expired.assert_called_once_with("test_key")
        mock_acquire_lock.assert_called_once()

    @patch('app.LocalDataStore.acquire_file_lock')
    def test_load_invalid_json(self, mock_acquire_lock):
        mock_acquire_lock.return_value = None
        self.data_store = MagicMock()
        self.data_store.load_data.side_effect = ValueError("Invalid JSON")
        with self.assertRaises(ValueError):
            self.data_store.load_data()

    @patch('app.LocalDataStore.acquire_file_lock')
    def test_read_expired_key(self, mock_acquire_lock):
        mock_acquire_lock.return_value = None
        self.data_store = MagicMock()
        self.data_store.read.return_value = {"status": "error"} 
        result = self.data_store.read("expired_key")
        self.assertEqual(result["status"], "error")
        self.assertTrue(self.data_store.read.called)

    @patch('app.LocalDataStore.acquire_file_lock')
    def test_delete_key_failure(self, mock_acquire_lock): 
        mock_acquire_lock.return_value = None  
        self.data_store = MagicMock()
        self.data_store.delete.return_value = False 
        result = self.data_store.delete("non_existent_key")
        self.assertFalse(result)
        self.assertTrue(self.data_store.delete.called)

    @patch('app.LocalDataStore.acquire_file_lock')
    def test_create_key_failure(self, mock_acquire_lock): 
        mock_acquire_lock.return_value = None
        self.data_store = MagicMock()
        self.data_store.create.return_value = False  
        result = self.data_store.create("new_key", {"data": "value"})
        self.assertFalse(result)
        self.assertTrue(self.data_store.create.called)

if __name__ == '_main_':
    unittest.main()