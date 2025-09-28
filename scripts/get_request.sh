#!/bin/bash

# Usage: ./get_request.sh <key>

URL='http://localhost:8080/api/get'

if [ $# -ne 1 ]; then
    echo "Usage: $0 key"
    exit 1
fi

KEY=$1

curl -X GET "$URL/$KEY" \
    -H "Content-Type: application/json"

echo
