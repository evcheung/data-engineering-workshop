#!/usr/bin/env bash

set -e

CONTAINER_NAME=workshop

export PATH=${PATH}:/app/spark/bin

function run {
    docker run --rm -it \
    --network=data-engineering-workshop_data-engineering-workshop-internal \
    ${CONTAINER_NAME}
}

case $1 in
    buildJar)
        sbt assembly
        ;;
    build)
        docker build . -t ${CONTAINER_NAME}
        ;;
    run)
        run
        ;;
    *)
        echo "help"
        ;;
esac


