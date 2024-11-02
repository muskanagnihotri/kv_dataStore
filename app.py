import os
import json
import threading
import time
from typing import Any, Dict, Optional
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom exceptions for better error handling
class DataStoreError(Exception):
    pass

class KeyExistsError(DataStoreError):
    pass

class KeyNotFoundError(DataStoreError):
    pass

class KeyTooLongError(DataStoreError):
    pass

class ValueTooLargeError(DataStoreError):
    pass

class LocalDataStore:
    MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB in bytes
    MAX_KEY_LENGTH = 32
    MAX_VALUE_SIZE = 16 * 1024  # 16KB in bytes
    BATCH_LIMIT = 100  # Maximum number of key-value pairs allowed in a batch operation

    def __init__(self, file_path: Optional[str] = None):
        # Initialize the data store with an optional file path
        default_path = os.path.join(os.path.expanduser('~'), 'Documents', 'data_store.json')
        self.file_path = file_path or default_path
        self.lock = threading.Lock()
        self.data = {}
        self.load_data()

    def load_data(self):
        """Load data from the specified file path."""
        logging.info("Loading data from %s", self.file_path)
        try:
            with open(self.file_path, 'r') as f:
              self.data = json.load(f)
              logging.info("Data loaded successfully.")
        except FileNotFoundError:
            logging.warning("Data file not found, creating a new empty data store.")
            self.data = {}
            # Create an empty data file
            with open(self.file_path, 'w') as f:
               json.dump({}, f)
        except json.JSONDecodeError:
            logging.error("Error decoding JSON from the data file.")
        except Exception as e:
            logging.error(f"An error occurred while loading data: {e}")
            
    def save_data(self):
        """Save data to the specified file path."""
        with self.lock:
            if os.path.exists(self.file_path) and os.path.getsize(self.file_path) >= self.MAX_FILE_SIZE:
                self.cleanup_expired_keys()  # Attempt cleanup of expired keys to free up space

            if os.path.getsize(self.file_path) >= self.MAX_FILE_SIZE:
                raise Exception("Error: Data file size limit reached. Cleanup required.")

            with open(self.file_path, 'w') as f:
                json.dump(self.data, f)
                logging.info("Data saved successfully.")

    def create(self, key: str, value: Dict, ttl: Optional[int] = None):
        """Create a new key-value pair with an optional TTL."""
        with self.lock:
            if key in self.data:
                raise KeyExistsError(f"Error: Key '{key}' already exists.")

            if len(key) > self.MAX_KEY_LENGTH:
                raise KeyTooLongError(f"Error: Key '{key}' exceeds maximum length of {self.MAX_KEY_LENGTH} characters.")

            if len(json.dumps(value)) > self.MAX_VALUE_SIZE:
                raise ValueTooLargeError(f"Error: Value for key '{key}' exceeds maximum size of {self.MAX_VALUE_SIZE} bytes.")

            expiry = time.time() + ttl if ttl else None
            self.data[key] = {'value': value, 'expiry': expiry}
            self.save_data()
            return f"Key '{key}' created successfully."

    def read(self, key: str) -> Optional[Dict]:
        """Retrieve the JSON value corresponding to a key."""
        with self.lock:
            if key not in self.data or self.is_expired(key):
                return "Error: Key not found or expired."
            return self.data[key]['value']

    def delete(self, key: str):
        """Remove a key-value pair using the key."""
        with self.lock:
            if key in self.data:
                del self.data[key]
                self.save_data()
                return f"Key '{key}' deleted successfully."
            else:
                return f"Error: Key '{key}' not found."

    def is_expired(self, key: str) -> bool:
        """Check if a key has expired."""
        expiry = self.data[key].get('expiry')
        if expiry and time.time() > expiry:
            del self.data[key]
            self.save_data()
            return True
        return False

    def cleanup_expired_keys(self):
        """Remove expired keys from the data store."""
        with self.lock:
            keys_to_delete = [key for key in self.data if self.is_expired(key)]
            for key in keys_to_delete:
                del self.data[key]
            self.save_data()

    def batch_create(self, kv_pairs: Dict[str, Dict], ttl: Optional[int] = None):
        """Create multiple key-value pairs in a single operation."""
        if len(kv_pairs) > self.BATCH_LIMIT:
            return f"Error: Batch limit exceeded. Max {self.BATCH_LIMIT} pairs allowed."

        errors = {}
        for key, value in kv_pairs.items():
            try:
                self.create(key, value, ttl)
            except DataStoreError as e:
                errors[key] = str(e)

        if errors:
            return f"Batch completed with errors: {errors}"
        return "Batch creation successful."



if __name__ == "__main__":
    print("Initializing data store...")
    store = LocalDataStore()
    print("Data store initialized.")
    
    print("Creating key1...")
    print(store.create("key1", {"name": "value1"}),ttl=5)
    
    print("Reading key1...")
    print(store.read("key1"))
    
    print("Sleeping for 6 seconds...")
    time.sleep(6)
    
    print("Reading key1 after TTL...")
    print(store.read("key1"))
    
    print("Deleting key1...")
    print(store.delete("key1"))
    
    print("Creating key2...")
    print(store.create("key2", {"name": "value2"}))
    
    print("Batch creating keys...")
    print(store.batch_create({"key3": {"name": "value3"}, "key4": {"name": "value4"}}, ttl=10))
    
    print("Reading key3...")
    print(store.read("key3"))
    
    print("Sleeping for 11 seconds...")
    time.sleep(11)
    
    print("Reading key3 after TTL...")
    print(store.read("key3"))
