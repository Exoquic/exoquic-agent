FROM openjdk:17-jdk-alpine

LABEL maintainer="Exoquic <support@exoquic.com>"
LABEL description="Exoquic PostgreSQL Agent for capturing and streaming database changes"

# Create app directory
WORKDIR /app

# Create volume for offsets and logs

# Copy JAR file
COPY target/exoquic-agent.jar /app/

# Set environment variables
ENV JAVA_OPTS="-Xms256m -Xmx512m"

# Required environment variables:
# PG_HOST - PostgreSQL host (default: localhost)
# PG_PORT - PostgreSQL port (default: 5432)
# PG_DATABASE - PostgreSQL database name
# PG_USER - PostgreSQL user
# PG_PASSWORD - PostgreSQL password
# PG_SCHEMA - PostgreSQL schema (default: public)
# REPLICATION_SLOT_NAME - Replication slot name (default: exoquic_agent_slot)
# PUBLICATION_NAME - Publication name (default: exoquic_agent_pub)
# EXOQUIC_BASE_URL - Exoquic API base URL
# EXOQUIC_API_KEY - Exoquic API key
#
# Optional environment variables:
# HTTP_CONNECTION_TIMEOUT - HTTP connection timeout in ms (default: 5000)
# HTTP_SOCKET_TIMEOUT - HTTP socket timeout in ms (default: 30000)
# MAX_RETRIES - Maximum number of retries (default: 5)
# INITIAL_RETRY_DELAY_MS - Initial retry delay in ms (default: 1000)
# MAX_RETRY_DELAY_MS - Maximum retry delay in ms (default: 60000)
# POLL_INTERVAL_MS - Poll interval in ms (default: 1000)
# BATCH_SIZE - Batch size (default: 100)

# Create directory for logs
RUN mkdir -p /app/logs

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar exoquic-agent.jar"]
