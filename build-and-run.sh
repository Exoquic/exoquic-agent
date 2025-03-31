#!/bin/bash
set -e

echo "Building Exoquic PostgreSQL Agent..."
mvn clean package

echo "Creating logs directory..."
mkdir -p logs

echo "Running Exoquic PostgreSQL Agent..."
java -jar target/exoquic-agent.jar config.properties
