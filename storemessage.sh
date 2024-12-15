#!/bin/bash

# Verifica que se hayan pasado dos argumentos
if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <id> <message>"
    exit 1
fi

BLOCK_ID=$1
MESSAGE=$2


RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/storeblock" -H "Content-Type: application/json" -d "{\"block_id\": \"$BLOCK_ID\", \"data\": \"$MESSAGE\"}")
echo " $RESPONSE"
exit 0