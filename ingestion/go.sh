#!/usr/bin/env bash

set -e

function run {
    SCRIPT_NAME=$1
    docker run --rm -it
        -e KAFKA_SERVERS=kafka:9092 \
        --network=data-engineering-workshop_data-engineering-workshop-internal ingestion \
        python $SCRIPT_NAME
}

case $1 in
    build)
        docker build . -t ingestion
        ;;
    orders)
        run orders.py
        ;;
    *)
        echo "help"
        ;;
esac