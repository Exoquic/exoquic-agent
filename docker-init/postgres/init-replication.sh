#!/bin/bash
set -e

# Create the replication slot
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT pg_create_logical_replication_slot('exoquic_slot', 'pgoutput');
EOSQL

# Create the publication
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE PUBLICATION exoquic_pub FOR ALL TABLES;
EOSQL

# Create a test table and insert some data
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO test_table (name, description) VALUES
        ('Item 1', 'Description for item 1'),
        ('Item 2', 'Description for item 2'),
        ('Item 3', 'Description for item 3');
EOSQL

echo "PostgreSQL initialized with replication slot, publication, and test data"
