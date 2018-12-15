#!/usr/bin/env bash

set -e

CONTAINER_ANME=workshop

export PATH=${PATH}:/app/spark/bin

function run {
    docker run --rm -it \
    --network=data-engineering-workshop_data-engineering-workshop-internal \
    $CONTAINER_ANME
}

case $1 in
    build)
        docker build . -t $CONTAINER_ANME
        ;;
    run)
        run
        ;;
    *)
        echo "help"
        ;;
esac


