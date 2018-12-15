#!/usr/bin/env bash

set -e

if [ "$1" = '' ]
then
    mkdir -p /tmp/spark-events

    /spark/bin/spark-submit --class workshop.station.Station \
        --master spark://spark:7077 \
        --conf spark.eventLog.enabled=true \
        app.jar
else
    eval "$@"
fi