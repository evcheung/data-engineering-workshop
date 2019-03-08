#!/usr/bin/env bash

set -e

if [ "$1" = "" ]
then
    bin/kafka-server-start.sh config/server.properties
else
	eval "$@"
fi