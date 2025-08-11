# Mini LSM-Tree Table API Server

LSM-Tree (Log-Structured Merge Tree) is a data structure designed for high write throughput and efficient reads. This API server provides a simple key-value store based on the LSM-Tree architecture.

## What is LSM-Tree?

LSM-Tree (Log-Structured Merge Tree) is a write-optimized data structure commonly used in modern key-value stores such as LevelDB, RocksDB, and Cassandra. It is designed to deliver:

High write throughput by buffering writes in memory and flushing them to disk in batches.

Efficient reads by organizing data into sorted files and using indexes and Bloom filters to reduce disk I/O.

Instead of writing data directly to disk for every operation, LSM-Trees first store data in memory (e.g., a MemTable) and periodically flush it to disk as immutable sorted files called SSTables (Sorted String Tables). This minimizes random writes and maximizes sequential disk access.

## Quick Start

You can easily start the API server using Docker and Makefile:

```bash
make up
```

The server will start on port `8080` by default.
To stop the server, run:

```bash
make down
```


## API Endpoints

### 1. Store Data
```bash
curl -X PUT http://localhost:8080/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "value": "Alice"}'
```

**Response:**
```json
{
  "status": "success",
  "message": "Key 'user:1' stored successfully"
}
```

### 2. Retrieve Data
```bash
curl http://localhost:8080/api/get/user:1
```

**Response:**
```json
{
  "key": "user:1",
  "value": "Alice",
  "found": true
}
```

### 3. Delete Data
```bash
curl -X DELETE http://localhost:8080/api/delete \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1"}'
```

**Response:**
```json
{
  "status": "success",
  "message": "Key 'user:1' deleted successfully"
}
```

### 4. System Status
```bash
curl http://localhost:8080/api/status
```

**Response:**
```json
{
  "active_memtable_size": 3,
  "immutable_count": 1,
  "sstable_stats": {
    "level_0": 2,
    "level_1": 1
  },
  "message": "LSM-Tree service is running"
}
```

### 5. Health Check
```bash
curl http://localhost:8080/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "mini-lsm-table"
}
```

### 6. Trigger Recovery
```bash
curl -X POST http://localhost:8080/api/recovery
```

**Response:**
```json
{
  "status": "success",
  "message": "Recovery completed successfully"
}
```


## Error Responses

The API returns appropriate HTTP status codes and JSON error messages:

```json
{
  "error": "Key cannot be empty"
}
```

### HTTP Status Codes
- `200 OK`: Successful operation
- `400 Bad Request`: Invalid request format or missing required fields
- `404 Not Found`: Key not found (for GET operations)
- `405 Method Not Allowed`: Incorrect HTTP method
- `500 Internal Server Error`: Server-side error

## Data Directory

By default, the server stores data in a temporary directory that persists between runs:
- **Location**: `/tmp/mini_lsm_api/`
- **Structure**:
  - `wal/`: Write-Ahead Log files
  - `sstables/`: SSTable files organized by levels

## Architecture

The API server is built on top of a LSM-Tree storage engine with the following components:

- **MemTable**: In-memory sorted tree for recent writes
- **SSTable**: Sorted String Tables for persistent storage
- **WAL**: Write-Ahead Log for durability
- **Compaction**: Background process to merge and optimize SSTables
- **Block Index**: Efficient key lookup within SSTables
- **Bloom Filter**: Probabilistic data structure to avoid unnecessary disk reads
