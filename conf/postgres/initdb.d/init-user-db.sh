#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER docker;
    CREATE DATABASE docker;
    GRANT ALL PRIVILEGES ON DATABASE docker TO docker;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "docker" <<-EOSQL
    CREATE SEQUENCE orders_id_seq;
    CREATE TABLE orders (
        id INTEGER NOT NULL DEFAULT nextval('orders_id_seq'),
        "orderId" VARCHAR NOT NULL,
        "itemId" VARCHAR NOT null,
        quantity INTEGER,
        price INTEGER,
        "timestamp" TIMESTAMP,
        PRIMARY KEY (id)
    );
    ALTER SEQUENCE orders_id_seq OWNED BY orders.id;

    CREATE SEQUENCE kafka_orders_id_seq;
    CREATE TABLE kafka_orders (
        id INTEGER NOT NULL DEFAULT nextval('kafka_orders_id_seq'),
        "itemId" VARCHAR NOT null,
        "count" INTEGER,
        "startTime" TIMESTAMP,
        "endTime" TIMESTAMP,
        PRIMARY KEY (id)
    );
    ALTER SEQUENCE kafka_orders_id_seq OWNED BY kafka_orders.id;
EOSQL