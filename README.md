# Mini LSM-Tree Table API Server

A high-performance HTTP API server built on top of the LSM-Tree (Log-Structured Merge Tree) storage engine.

## Features

- 🚀 **Fast Key-Value Operations**: PUT, GET, DELETE operations
- 📊 **System Monitoring**: Real-time statistics and health checks
- 🔄 **Data Recovery**: Automatic recovery from WAL (Write-Ahead Log)
- 🌐 **RESTful API**: JSON-based HTTP endpoints
- 💾 **Persistent Storage**: Data persists between server restarts

## Quick Start

### Running the Server

```bash
go run cmd/main.go
```

The server will start on port `8080` by default. You can change the port by setting the `PORT` environment variable:

```bash
PORT=3000 go run cmd/main.go
```

### API Documentation

Once the server is running, visit `http://localhost:8080/` to see the complete API documentation.

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

## Example Usage Scenarios

### Basic CRUD Operations
```bash
# Store user data
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "user:alice", "value": "Alice Johnson"}'
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "user:bob", "value": "Bob Smith"}'

# Retrieve user data
curl http://localhost:8080/api/get/user:alice
curl http://localhost:8080/api/get/user:bob

# Update user data (overwrite)
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "user:alice", "value": "Alice Johnson-Brown"}'

# Delete user data
curl -X DELETE http://localhost:8080/api/delete -H "Content-Type: application/json" -d '{"key": "user:bob"}'

# Check if deleted key exists
curl http://localhost:8080/api/get/user:bob
```

### Configuration Management
```bash
# Store configuration
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "config:database:host", "value": "localhost:5432"}'
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "config:cache:redis", "value": "redis:6379"}'
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "config:app:debug", "value": "true"}'

# Retrieve configuration
curl http://localhost:8080/api/get/config:database:host
curl http://localhost:8080/api/get/config:cache:redis
curl http://localhost:8080/api/get/config:app:debug
```

### Session Management
```bash
# Store session data
curl -X PUT http://localhost:8080/api/put -H "Content-Type: application/json" -d '{"key": "session:abc123", "value": "{\"user_id\": 42, \"expires\": \"2024-12-31\"}"}'

# Retrieve session
curl http://localhost:8080/api/get/session:abc123

# Delete expired session
curl -X DELETE http://localhost:8080/api/delete -H "Content-Type: application/json" -d '{"key": "session:abc123"}'
```

## Performance Testing

You can test the server performance with many concurrent requests:

```bash
# Install hey (HTTP load testing tool)
go install github.com/rakyll/hey@latest

# Test PUT operations
hey -n 1000 -c 10 -m PUT -H "Content-Type: application/json" -d '{"key": "test:key", "value": "test value"}' http://localhost:8080/api/put

# Test GET operations (after storing some data)
hey -n 1000 -c 10 http://localhost:8080/api/get/test:key
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

## Development

### Building
```bash
go build -o mini-lsm-server cmd/main.go
./mini-lsm-server
```

### Testing
```bash
go test ./...
```

### Docker Support
```bash
# Build image
docker build -t mini-lsm-server .

# Run container
docker run -p 8080:8080 mini-lsm-server
```
