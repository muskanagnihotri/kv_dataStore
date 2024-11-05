import os
import json
import threading
import time
from typing import Any, Dict, Optional
import logging
import shutil
# import fcntl  # For file locking on Unix
import msvcrt  # For file locking on Windows

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

class InvalidJSONError(DataStoreError):
    pass

class FileSizeLimitExceededError(DataStoreError):
    pass

class LocalDataStore:
    MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB in bytes
    MAX_KEY_LENGTH = 32
    MAX_VALUE_SIZE = 16 * 1024  # 16KB in bytes
    BATCH_LIMIT = 100  # Maximum number of key-value pairs allowed in a batch operation
    MAX_DATA_CAPACITY = int(MAX_FILE_SIZE * 0.9)  # Max capacity for self.data (90% of file limit)

    def _init_(self, file_path: Optional[str] = None):
        # Initialize the data store with an optional file path
        default_path = os.path.join(os.path.expanduser('~'), 'Documents', 'data_store.json')
        self.file_path = file_path or default_path
        self.lock = threading.Lock()  # Thread lock for critical section
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
            self.save_data()  # Initialize with an empty file
        except json.JSONDecodeError:
            backup_path = f"{self.file_path}.backup"
            logging.error("Invalid JSON in data file. Creating a backup and initializing an empty data store.")
            shutil.move(self.file_path, backup_path)
            logging.info("Backup created at %s", backup_path)
            self.data = {}
            self.save_data()
            raise InvalidJSONError(
                f"Error: The data file at {self.file_path} contains invalid JSON. A backup has been created at {backup_path}. "
                "The data store has been reset. Please check the backup file for errors or reinitialize with a valid JSON file."
            )
        except Exception as e:
            logging.error(f"An error occurred while loading data: {e}")
            raise DataStoreError(f"An unexpected error occurred: {e}")

    def save_data(self):
        """Save data to the specified file path, managing file size growth."""
        self.enforce_file_size_limit()  # Ensure space is available before adding new entry

        # Implement file locking for exclusive access
        with open(self.file_path, 'r+') as f:
            if os.name == 'posix':  # Unix/Linux
                fcntl.flock(f, fcntl.LOCK_EX)  # Acquire an exclusive lock
            elif os.name == 'nt':  # Windows
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, os.path.getsize(self.file_path))

            # Thread-safe writing operation
            with self.lock:
                f.seek(0)  # Move the pointer to the beginning of the file
                json.dump(self.data, f)
                f.truncate()  # Truncate the file to the new size
                logging.info("Data saved successfully.")

    def enforce_file_size_limit(self):
        """Manage data capacity and clear expired keys if file size exceeds threshold."""
        current_size = len(json.dumps(self.data).encode('utf-8'))

        if current_size >= self.MAX_DATA_CAPACITY:
            logging.info("Data capacity nearing limit. Cleaning up expired keys.")
            self.cleanup_expired_keys()

            # Re-check size after cleanup
            current_size = len(json.dumps(self.data).encode('utf-8'))
            if current_size >= self.MAX_DATA_CAPACITY:
                logging.error("Data store has exceeded the capacity limit after cleanup.")
                raise FileSizeLimitExceededError("Error: Data store capacity exceeded. Delete some entries to free up space.")

    def create(self, key: str, value: Dict, ttl: Optional[int] = None):
        """Create a new key-value pair with an optional TTL."""
        # Perform input checks outside lock
        if len(key) > self.MAX_KEY_LENGTH:
            raise KeyTooLongError(f"Error: Key '{key}' exceeds maximum length of {self.MAX_KEY_LENGTH} characters.")

        if len(json.dumps(value)) > self.MAX_VALUE_SIZE:
            raise ValueTooLargeError(f"Error: Value for key '{key}' exceeds maximum size of {self.MAX_VALUE_SIZE} bytes.")

        expiry = time.time() + ttl if ttl else None

        # Critical section for data modification
        with self.lock:
            if key in self.data:
                raise KeyExistsError(f"Error: Key '{key}' already exists.")

            self.data[key] = {'value': value, 'expiry': expiry}

        # Save data outside of lock to prevent holding the lock too long
        self.save_data()
        return f"Key '{key}' created successfully."

    def read(self, key: str) -> Dict[str, Any]:
        """Retrieve the JSON value corresponding to a key and differentiate expired keys."""
        with self.lock:
            if key not in self.data:
                return {
                    "status": "error",
                    "message": f"Key '{key}' not found."
                }
            elif self.is_expired(key):
                return {
                    "status": "expired",
                    "message": f"Key '{key}' has expired."
                }
            else:
                return {
                    "status": "success",
                    "value": self.data[key]['value']
                }

    def delete(self, key: str):
        """Remove a key-value pair using the key."""
        with self.lock:
            if key in self.data:
                del self.data[key]
                self.save_data()
                return f"Key '{key}' deleted successfully."
            else:
                return f"Error: Key '{key}' not found."

    def is_key_expired(self, key: str) -> bool:
        """Check if a key has expired without deleting it."""
        expiry = self.data[key].get('expiry')
        return expiry is not None and time.time() > expiry

    def is_expired(self, key: str) -> bool:
        """Check if a key has expired and delete it if so."""
        if self.is_key_expired(key):
            del self.data[key]
            self.save_data()
            return True
        return False

    def cleanup_expired_keys(self):
        """Remove expired keys from the data store."""
        expired_keys = [key for key in self.data if self.is_key_expired(key)]

        # Perform deletions in a single pass
        if expired_keys:
            with self.lock:
                for key in expired_keys:
                    del self.data[key]
                self.save_data()

    def batch_create(self, kv_pairs: Dict[str, Dict], ttl: Optional[int] = None) -> Dict[str, Any]:
        """Create multiple key-value pairs in a single operation and return detailed results."""
        if len(kv_pairs) > self.BATCH_LIMIT:
            return {
                "status": "error",
                "message": f"Batch limit exceeded. Maximum {self.BATCH_LIMIT} pairs allowed.",
            }

        results = {
            "created": [],   # To store keys successfully created
            "errors": {}     # To store any errors for individual keys
        }

        for key, value in kv_pairs.items():
            try:
                self.create(key, value, ttl)
                results["created"].append(key)  # Add key to successful creations
            except DataStoreError as e:
                results["errors"][key] = str(e)  # Add error message for the specific key

        # Determine overall status based on the results
        results["status"] = "success" if not results["errors"] else "partial_success"
        results["message"] = (
            "Batch creation completed with some errors." if results["errors"] 
            else "Batch creation successful."
        )

        return results

