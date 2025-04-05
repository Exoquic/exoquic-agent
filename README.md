# Exoquic PostgreSQL Agent

A lightweight agent that captures data changes from PostgreSQL databases and streams them to Exoquic. The agent uses PostgreSQL's logical replication to capture database changes (inserts, updates, deletes) in real-time and sends them to Exoquic's Kafka HTTP Bridge.

## Features

- **Real-time Change Data Capture**: Captures database changes as they happen using PostgreSQL's logical replication
- **Automatic PostgreSQL Configuration**: Validates and configures PostgreSQL settings automatically
- **Lightweight**: Uses Debezium's embedded engine without requiring a full Kafka Connect deployment
- **Secure**: Establishes outbound-only connections, ensuring your databases remain secure behind your firewalls
- **Resilient**: Includes automatic reconnection, retry mechanisms, and error handling
- **Configurable**: Supports extensive configuration through environment variables
- **Containerized**: Available as both a standalone JAR and Docker container

## Requirements

- Java 17 or later
- PostgreSQL 10 or later

## Installation

### Using the JAR file

1. Download the latest JAR file from the releases page
2. Set the required environment variables (see [Configuration](#configuration))
3. Run the agent:

```bash
java -jar exoquic-agent.jar
```

### Using Docker

1. Pull the Docker image:

```bash
docker pull exoquic/postgres-agent
```

2. Run the container with environment variables:

```bash
docker run \
  -e PG_HOST=localhost \
  -e PG_PORT=5432 \
  -e PG_DATABASE=mydb \
  -e PG_USER=postgres \
  -e PG_PASSWORD=postgres \
  -e EXOQUIC_API_KEY=your-api-key-here \
  exoquic/postgres-agent
```
## Automatic PostgreSQL Configuration

The agent includes an automatic configuration validator that ensures your PostgreSQL server is properly configured for logical replication. On startup, it:

### 1. WAL Settings
- Validates and sets `wal_level` to 'logical'
- Ensures `max_replication_slots` is at least 5
- Ensures `max_wal_senders` is at least 5
- Automatically applies these settings if needed
- Waits for settings to take effect after PostgreSQL restart

### 2. User Permissions
- Verifies the user has replication permission
- Checks for USAGE permission on the public schema
- Validates table access rights

### 3. Replication Infrastructure
- Creates or validates the replication slot
- Creates or validates the publication for all tables
- Sets REPLICA IDENTITY FULL for tables without primary keys

### 4. Resilient Configuration
- Handles PostgreSQL restarts gracefully
- Retries configuration checks for up to 2 minutes
- Provides detailed feedback about required changes
- Automatically reconnects after connection loss

The validator will:
1. First attempt to set any missing configurations
2. If changes require a restart, it will notify you
3. Wait for settings to take effect after restart
4. Continue with replication setup once all settings are correct

## Configuration

The agent is configured entirely through environment variables.

### Required Environment Variables

#### PostgreSQL Settings
- `PG_HOST` - Database host (default: localhost)
- `PG_PORT` - Database port (default: 5432)
- `PG_DATABASE` - Database name
- `PG_USER` - Username
- `PG_PASSWORD` - Password
- `PG_SCHEMA` - Schema name (default: public)

#### Exoquic Settings
- `EXOQUIC_API_KEY` - Your Exoquic API key

### Optional Environment Variables

#### Replication Settings
- `REPLICATION_SLOT_NAME` - Replication slot name (default: exoquic_agent_slot)
- `PUBLICATION_NAME` - Publication name (default: exoquic_agent_pub)

#### HTTP Settings
- `HTTP_CONNECTION_TIMEOUT` - HTTP connection timeout in ms (default: 5000)
- `HTTP_SOCKET_TIMEOUT` - HTTP socket timeout in ms (default: 30000)

#### Retry Settings
- `MAX_RETRIES` - Maximum number of retries (default: 5)
- `INITIAL_RETRY_DELAY_MS` - Initial retry delay in ms (default: 1000)
- `MAX_RETRY_DELAY_MS` - Maximum retry delay in ms (default: 60000)

#### Performance Settings
- `POLL_INTERVAL_MS` - Poll interval in ms (default: 1000)
- `BATCH_SIZE` - Batch size (default: 100)

## Building from Source

1. Clone this repository
2. Build the project using Maven:

```bash
mvn clean package
```

3. The JAR file will be created in the `target` directory

## Event Format

The agent sends events to Exoquic in the following JSON format:

```json
{
  "type": "created|updated|removed",
  "data": {
    // The row data
  }
}
```

## Monitoring and Logging

The agent logs to both the console and a rolling file in the `logs` directory. The log level can be configured in the `log4j2.xml` file.

## Troubleshooting

### Common Issues

1. **Connection refused**: Make sure PostgreSQL is running and accessible from the agent
2. **Permission denied**: Check that the database user has the necessary permissions
3. **Replication slot not found**: Ensure the replication slot exists and is not in use
4. **Publication not found**: Verify that the publication exists and includes the tables you want to monitor
5. **WAL settings not taking effect**: After the agent modifies PostgreSQL settings, you may need to restart the PostgreSQL server. The agent will wait for the settings to take effect.

### Configuration Validation Messages

The agent provides detailed feedback about PostgreSQL configuration:

```
INFO: Checking PostgreSQL configuration...
INFO: Setting wal_level to logical
WARNING: PostgreSQL restart required for new settings to take effect
INFO: Waiting for settings to be applied...
INFO: All required settings configured successfully
```

### Checking Logs

The agent logs to both the console and a rolling file in the `logs` directory. Check these logs for error messages and stack traces.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please contact support@exoquic.com or open an issue on GitHub.
