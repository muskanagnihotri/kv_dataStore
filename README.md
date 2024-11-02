# Key-Value Data Store
This project implements a local data store for managing key-value pairs with optional TTL (Time-To-Live) functionality. It provides basic operations like create, read, delete, and batch create, and handles concurrency using threading locks.

# Features
Create key-value pairs with optional TTL.

Read key-value pairs.

Delete key-value pairs.

Batch create key-value pairs.

Automatic cleanup of expired keys.

JSON file-based storage for persistence.

Error handling with custom exceptions.

By default, the data store will save the data file (data_store.json) in the user's Documents directory. 

# Usage Instructions
1-Initialize the data store: store = LocalDataStore()

2-Create a key-value pair with optional TTL (in seconds):store.create("key1", {"name": "value1"}, ttl=5)

3-Read a key-value pair:value = store.read("key1")

4-Delete a key-value pair:store.delete("key1")

5-Batch create key-value pairs with optional TTL:
kv_pairs = {"key2": {"name": "value2"}, "key3": {"name": "value3"}}
store.batch_create(kv_pairs, ttl=10)

# Running the Script-
python app.py

# Testing
The main script includes test cases that demonstrate the functionality of the data store. These test cases cover creating keys, reading values, handling TTL expiry, deleting keys, and batch operations.

# Design Decisions
1-Thread Safety: The threading.Lock is used to ensure thread-safe operations for concurrent access.

2-TTL Handling: Expiry time is calculated and stored with each key-value pair. The data store automatically cleans up expired keys during read, write, and batch operations.

3-Error Handling: Custom exceptions (DataStoreError, KeyExistsError, KeyNotFoundError, KeyTooLongError, ValueTooLargeError) are used for clear and specific error messages.

4-File Storage: JSON file is used to persist the data. The file size is limited to 1GB to ensure manageable storage.

# System-Specific Dependencies or Limitations
File Size Limitation: The maximum file size for the data store is set to 1GB. If the file exceeds this size, cleanup of expired keys will be attempted. If it still exceeds the limit, an error is raised.

File Path: Default file path is set to the user's Documents directory for cross-platform compatibility. Custom file paths can be specified if needed.

