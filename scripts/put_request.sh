#!/bin/bash

# Usage: ./put_request.sh <key> <value>

URL='http://localhost:8080/api/put'

# Check if the number of arguments is even
if [ $# -ne 2 ]; then
    echo "Usage: $0 key value"
    exit 1
fi

KEY=$1
VALUE=$2

DATA=$(printf '{"key":"%s","value":"%s"}' "$KEY" "$VALUE")

curl -X PUT "$URL" \
    -H "Content-Type: application/json" \
    -d "$DATA"

echo