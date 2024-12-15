#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <message_id>"
    exit 1
fi

block_id=$1
RESPONSE=$(curl "http://127.0.0.1:8080/readblock?type=ReadBlock&block_id=${block_id}")
echo "$RESPONSE"