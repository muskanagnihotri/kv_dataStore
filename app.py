import os
import json
import threading
import time
from typing import Any, Dict, Optional
import logging
import shutil
import platform

if platform.system() == 'Windows':
    import msvcrt
else:
    import fcntl

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
    WARNING_THRESHOLD = 0.90  # 90% capacity usage
    CRITICAL_THRESHOLD = 0.98  # 98% capacity usage

    def __init__(self, file_path: Optional[str] = None, monitor_interval: int = 60):
        # Initialize the data store with an optional file path
        default_path = os.path.join(os.path.expanduser('~'), 'Documents', 'data_store.json')
        self.file_path = file_path or default_path
        self.lock = threading.Lock()  # Thread lock for critical section
        self.data = {}
        self.load_data()
        self.monitor_interval = monitor_interval
        self.start_monitoring()
        self.file_lock = None

    def acquire_file_lock(self):
        """Acquire a file lock to prevent concurrent access"""
        try:
            self.file_lock = open(self.file_path, 'a+')  # Open the file in append mode

            if platform.system() == 'Windows':
                # For Windows, use msvcrt for file locking
                msvcrt.locking(self.file_lock.fileno(), msvcrt.LK_NBLCK, os.fstat(self.file_lock.fileno()).st_size)
            else:
                # For Unix-based systems, use fcntl for file locking
                fcntl.flock(self.file_lock, fcntl.LOCK_EX)  # Lock the file

        except Exception as e:
            logging.error(f"Error acquiring file lock: {e}")
            raise

    
    def release_file_lock(self):
        """Release the file lock after finishing file operations"""
        try:
            if self.file_lock:
                if platform.system() == 'Windows':
                    # For Windows, unlock using msvcrt
                    logging.info("Attempting to release file lock.")
                    msvcrt.locking(self.file_lock.fileno(), msvcrt.LK_UNLCK, os.fstat(self.file_lock.fileno()).st_size)
                else:
                    # For Unix-based systems, unlock using fcntl
                    logging.info("Attempting to release file lock.")
                    fcntl.flock(self.file_lock, fcntl.LOCK_UN)  # Unlock the file
                logging.info("File lock released successfully.")
                self.file_lock.close()  # Close the file explicitly

        except Exception as e:
            logging.error(f"Error releasing file lock: {e}")
            raise 
    def __enter__(self):
        """Enter the runtime context related to this object."""
        self.acquire_file_lock()
        return self

    def _exit_(self, exc_type, exc_val, exc_tb):
        """Context manager exit to release the file lock."""
        if exc_type is not None:
            logging.error("An exception occurred: %s", exc_val)
            logging.debug("Traceback details:", exc_info=(exc_type, exc_val, exc_tb))
        self.release_file_lock()

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
        except PermissionError:
            logging.error("Permission denied for file %s. Please check file permissions.", self.file_path)
            raise DataStoreError(f"Permission denied for file {self.file_path}.")
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

    def save_data(self):
        """Save data to the specified file path, with file locking for safe access."""
        self.acquire_file_lock()
        try:
            with open(self.file_path, 'w') as f:
                json.dump(self.data, f)
                f.truncate()
            logging.info("Data saved successfully.")
        except Exception as e:
            logging.error(f"Error saving data: {e}")
            raise
        finally:
            self.release_file_lock()

    def check_capacity_usage(self) -> float:
        """Calculate the current data capacity usage as a percentage."""
        current_size = len(json.dumps(self.data).encode('utf-8'))
        return current_size / self.MAX_DATA_CAPACITY

    def handle_critical_threshold(self):
        """Handle actions like cleanup or alerting when capacity exceeds the critical threshold."""
        logging.info("Handling critical threshold. Taking actions such as cleanup or alerting.")
        self.cleanup_expired_keys()

    def start_monitoring(self):
        """Start the background monitoring thread."""
        def monitor():
            while True:
                with self.lock:
                    usage_percentage = self.check_capacity_usage()
                    if usage_percentage > self.CRITICAL_THRESHOLD:
                       logging.warning("Critical alert: Data store is over 98% capacity.")       
                       self.handle_critical_threshold()
                    elif usage_percentage > self.WARNING_THRESHOLD:
                       logging.warning("Warning: Data store is over 90% capacity.")
                time.sleep(self.monitor_interval)

        monitoring_thread = threading.Thread(target=monitor, daemon=True)
        monitoring_thread.start()  

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
        """Create a new key-value pair in the data store with an optional TTL."""  
        with self.lock:
            if key in self.data:
                raise KeyExistsError(f"Error: The key '{key}' already exists in the data store.")
            if len(key) > self.MAX_KEY_LENGTH:
                raise KeyTooLongError(f"Error: The key length exceeds the maximum limit of {self.MAX_KEY_LENGTH} characters.")
            if len(json.dumps(value)) > self.MAX_VALUE_SIZE:
                raise ValueTooLargeError(f"Error: The value size exceeds the maximum limit of {self.MAX_VALUE_SIZE} bytes.")
            expiry = time.time() + ttl if ttl else None
            self.data[key] = {"value": value, "expiry": expiry}
            logging.info(f"Created new key: {key}")
            self.save_data()

        return True

    def read(self, key: str) -> Dict[str, Any]:
        """Retrieve the JSON value corresponding to a key and differentiate expired keys."""
        with self.lock:
            if key not in self.data or self.is_key_expired(key):
                return {
                    "status": "error",
                    "message": f"Key '{key}' not found."
                }
            else:
                return {
                    "status": "success",
                    "value": self.data[key]['value']
                }

    def delete(self, key: str):
        """Delete a key-value pair."""
        with self.lock:
            if key not in self.data or self.is_key_expired(key):
                raise KeyNotFoundError(f"Error: Key '{key}' not found.")
            del self.data[key]
        self.save_data()
        return f"Key '{key}' deleted successfully."

    def is_key_expired(self, key: str) -> bool:
        """Check if a key has expired, without deleting it."""
        item = self.data.get(key)
        if not item or item["expiry"] is None:
            return False
        return item["expiry"] < time.time()

    def is_expired(self, key: str) -> bool:
        """Check if a key has expired and delete it if so."""
        if self.is_key_expired(key):
            del self.data[key]
            self.save_data()
            return True
        return False

    def cleanup_expired_keys(self):
        """
        Removes keys that have expired. Ensures thread-safety by locking the shared resource.
        """
        with self.lock:
            current_time = time.time()
            expired_keys = [key for key, data in self.data.items() if data["expiry"] and data["expiry"] < current_time]
            
            for key in expired_keys:
                del self.data[key]
                logging.info(f"Cleaned up expired key: {key}")
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
        if results["errors"]:
            results["status"] = "partial_success"
            results["message"] = "Batch creation completed with some errors."
        else:
            results["status"] = "success"
            results["message"] = "Batch creation successful."
        return results

