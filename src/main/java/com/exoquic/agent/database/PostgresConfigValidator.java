package com.exoquic.agent.database;

import com.exoquic.agent.config.AgentConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates and configures PostgreSQL settings required for logical replication.
 */
public class PostgresConfigValidator {
    private static final Logger logger = LogManager.getLogger(PostgresConfigValidator.class);
    private static final int MAX_RETRIES = 5;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(3);
    private final AgentConfig config;

    public PostgresConfigValidator(AgentConfig config) {
        this.config = config;
    }

    /**
     * Validates and configures PostgreSQL settings.
     * 
     * @return ValidationResult containing the validation status and messages
     */
    public ValidationResult validateAndConfigure() {
        ValidationResult result = new ValidationResult();
        Connection conn = null;
        int attempts = 0;
        boolean configurationComplete = false;

        while (!configurationComplete) {
            try {
                if (attempts > 0) {
                    logger.info("Waiting 3 seconds before retrying configuration (attempt {})", attempts + 1);
                    Thread.sleep(3000);
                }

                // Try to connect or reconnect
                if (conn == null || conn.isClosed() || !conn.isValid(1)) {
                    try {
                        if (conn != null && !conn.isClosed()) {
                            conn.close();
                        }
                    } catch (SQLException e) {
                        // Ignore errors while closing connection
                    }

                    conn = connectWithRetry();
                    if (conn == null) {
                        logger.warn("Failed to connect on attempt {}", attempts + 1);
                        attempts++;
                        continue;
                    }
                    logger.info("Successfully connected to PostgreSQL");
                }

                // Check user permissions first
                try {
                    validatePermissions(conn, result);
                    if (!result.isSuccess()) {
                        return result; // Exit early if permissions are insufficient
                    }
                } catch (SQLException e) {
                    logger.warn("Error checking permissions (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Configure WAL settings
                try {
                    validateWALSettings(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating WAL settings (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Check/create publication
                try {
                    validatePublication(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating publication (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Check/create replication slot
                try {
                    validateReplicationSlot(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating replication slot (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Set REPLICA IDENTITY FULL for tables without primary keys
                try {
                    validateReplicaIdentity(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating replica identity (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Generate connection info
                try {
                    generateConnectionInfo(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error generating connection info (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                configurationComplete = true;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result.addError("Configuration interrupted: " + e.getMessage());
                return result;
            } catch (Exception e) {
                logger.warn("Unexpected error during configuration (attempt {}): {}", attempts + 1, e.getMessage());
                attempts++;
            } finally {
                if (configurationComplete && conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        logger.error("Error closing database connection", e);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Connects to PostgreSQL with retry logic.
     */
    private Connection connectWithRetry() {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                config.getDbHost(), config.getDbPort(), config.getDbName());
        
        Exception lastException = null;
        Duration retryDelay = INITIAL_RETRY_DELAY;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                logger.info("Attempting to connect to PostgreSQL (attempt {}/{})", attempt, MAX_RETRIES);
                Connection conn = DriverManager.getConnection(url, config.getDbUser(), config.getDbPassword());
                conn.setAutoCommit(true);
                logger.info("Successfully connected to PostgreSQL");
                return conn;
            } catch (SQLException e) {
                lastException = e;
                logger.warn("Failed to connect (attempt {}/{}): {}", attempt, MAX_RETRIES, e.getMessage());
                
                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(retryDelay.toMillis());
                        retryDelay = retryDelay.multipliedBy(2); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        logger.error("Failed to connect after {} attempts", MAX_RETRIES, lastException);
        return null;
    }

    /**
     * Validates user permissions.
     */
    private void validatePermissions(Connection conn, ValidationResult result) throws SQLException {
        // Check if user has replication permission
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT rolreplication FROM pg_roles WHERE rolname = current_user")) {
            if (rs.next() && !rs.getBoolean(1)) {
                result.addError("Current user does not have replication permission");
                return;
            }
        }

        // Check if user has required permissions on the schema
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT has_schema_privilege(current_user, 'public', 'USAGE')")) {
            if (rs.next() && !rs.getBoolean(1)) {
                result.addError("Current user does not have USAGE permission on public schema");
                return;
            }
        }

        result.addInfo("User permissions validated successfully");
    }

    /**
     * Validates and configures WAL settings, waiting for them to be properly set.
     * Handles connection issues during PostgreSQL restart.
     */
    private void validateWALSettings(Connection initialConn, ValidationResult result) throws SQLException {
        boolean walSettingsValid = false;
        int attempts = 0;
        Connection conn = initialConn;
        
        try {
            // First attempt to set parameters with initial connection
            checkAndSetWALParameters(conn, result, true);
        } catch (SQLException e) {
            logger.warn("Error during initial WAL parameter check: {}", e.getMessage());
            // Continue to retry loop
        }
        
        while (!walSettingsValid) {
            try {
                if (attempts > 0) {
                    logger.info("Waiting 3 seconds before checking WAL settings again");
                    Thread.sleep(3000);
                    
                    // Try to reconnect if connection is closed or invalid
                    if (conn == null || conn.isClosed() || !conn.isValid(1)) {
                        try {
                            if (conn != null && !conn.isClosed()) {
                                conn.close();
                            }
                        } catch (SQLException e) {
                            // Ignore errors while closing connection
                        }
                        
                        try {
                            conn = connectWithRetry();
                            if (conn != null) {
                                logger.info("Successfully reconnected to PostgreSQL");
                            }
                        } catch (Exception e) {
                            logger.debug("Failed to reconnect to PostgreSQL: {}", e.getMessage());
                            attempts++;
                            continue; // Skip this attempt if we can't connect
                        }
                    }
                }
                
                if (conn == null) {
                    logger.debug("No connection available, skipping attempt {}", attempts + 1);
                    attempts++;
                    continue;
                }
                
                attempts++;
                walSettingsValid = checkAndSetWALParameters(conn, result, false);
                
                if (walSettingsValid) {
                    result.addInfo("WAL settings validated successfully after " + attempts + " attempts");
                }
                
            } catch (SQLException e) {
                logger.debug("Database error during WAL check (attempt {}): {}", attempts, e.getMessage());
                // Continue the loop to retry
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting to check WAL settings", e);
            }
        }
    }
    
    /**
     * Checks and optionally sets WAL parameters.
     * Returns true if all parameters are valid.
     */
    private boolean checkAndSetWALParameters(Connection conn, ValidationResult result, boolean setParameters) throws SQLException {
        boolean allValid = true;
        
        // Check wal_level
        String walLevel = getPostgresParameter(conn, "wal_level");
        if (!"logical".equals(walLevel)) {
            allValid = false;
            if (setParameters) {
                setPostgresParameter(conn, "wal_level", "logical", result);
            }
            logger.info("Waiting for wal_level to be set to 'logical' (current: {})", walLevel);
        }

        // Check max_replication_slots
        int maxReplicationSlots = Integer.parseInt(getPostgresParameter(conn, "max_replication_slots"));
        if (maxReplicationSlots < 5) {
            allValid = false;
            if (setParameters) {
                setPostgresParameter(conn, "max_replication_slots", "5", result);
            }
            logger.info("Waiting for max_replication_slots to be >= 5 (current: {})", maxReplicationSlots);
        }

        // Check max_wal_senders
        int maxWalSenders = Integer.parseInt(getPostgresParameter(conn, "max_wal_senders"));
        if (maxWalSenders < 5) {
            allValid = false;
            if (setParameters) {
                setPostgresParameter(conn, "max_wal_senders", "5", result);
            }
            logger.info("Waiting for max_wal_senders to be >= 5 (current: {})", maxWalSenders);
        }
        
        return allValid;
    }

    /**
     * Validates and creates publication if needed.
     */
    private void validatePublication(Connection conn, ValidationResult result) throws SQLException {
        String publicationName = config.getPublicationName();
        
        // Check if publication exists
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_publication WHERE pubname = '" + publicationName + "'")) {
            if (!rs.next()) {
                // Create publication for all tables
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE PUBLICATION " + publicationName + " FOR ALL TABLES");
                    result.addInfo("Created publication: " + publicationName);
                }
            } else {
                result.addInfo("Publication already exists: " + publicationName);
            }
        }
    }

    /**
     * Validates and creates replication slot if needed.
     */
    private void validateReplicationSlot(Connection conn, ValidationResult result) throws SQLException {
        String slotName = config.getReplicationSlotName();
        
        // Check if slot exists
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
            if (!rs.next()) {
                // Create replication slot
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                    result.addInfo("Created replication slot: " + slotName);
                }
            } else {
                result.addInfo("Replication slot already exists: " + slotName);
            }
        }
    }

    /**
     * Sets REPLICA IDENTITY FULL for tables without primary keys.
     */
    private void validateReplicaIdentity(Connection conn, ValidationResult result) throws SQLException {
        List<String> tablesWithoutPK = new ArrayList<>();
        
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT schemaname, tablename FROM pg_tables t " +
                "WHERE schemaname = 'public' " +
                "AND NOT EXISTS (" +
                "    SELECT 1 FROM information_schema.table_constraints c " +
                "    WHERE c.table_schema = t.schemaname " +
                "    AND c.table_name = t.tablename " +
                "    AND c.constraint_type = 'PRIMARY KEY'" +
                ")")) {
            
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                tablesWithoutPK.add(tableName);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE public." + tableName + " REPLICA IDENTITY FULL");
                    result.addInfo("Set REPLICA IDENTITY FULL for table: " + tableName);
                }
            }
        }

        if (!tablesWithoutPK.isEmpty()) {
            result.addWarning("Tables without primary keys (REPLICA IDENTITY FULL set): " + String.join(", ", tablesWithoutPK));
        }
    }

    /**
     * Generates connection information summary.
     */
    private void generateConnectionInfo(Connection conn, ValidationResult result) throws SQLException {
        String listenAddresses = getPostgresParameter(conn, "listen_addresses");
        String port = getPostgresParameter(conn, "port");

        // If listen_addresses is '*', use the connection host
        if ("*".equals(listenAddresses)) {
            listenAddresses = config.getDbHost();
        }

        result.addInfo(String.format("""
            
            Exoquic Connection Information:
            ===========================
            Host: %s
            Port: %s
            Database: %s
            Username: %s
            Replication Slot: %s
            Publication: %s
            """,
            listenAddresses,
            port,
            config.getDbName(),
            config.getDbUser(),
            config.getReplicationSlotName(),
            config.getPublicationName()
        ));
    }

    /**
     * Gets a PostgreSQL parameter value.
     */
    private String getPostgresParameter(Connection conn, String paramName) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SHOW " + paramName)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            throw new SQLException("Parameter not found: " + paramName);
        }
    }

    /**
     * Sets a PostgreSQL parameter value.
     */
    private void setPostgresParameter(Connection conn, String paramName, String value, ValidationResult result) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER SYSTEM SET " + paramName + " = '" + value + "'");
            result.addInfo("Changed " + paramName + " to: " + value);
        }
    }

    /**
     * Validation result class.
     */
    public static class ValidationResult {
        private final List<String> messages = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();

        public void addInfo(String message) {
            messages.add("INFO: " + message);
        }

        public void addWarning(String message) {
            warnings.add("WARNING: " + message);
        }

        public void addError(String message) {
            errors.add("ERROR: " + message);
        }


        public boolean isSuccess() {
            return errors.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            // Add errors first
            errors.forEach(error -> sb.append(error).append('\n'));
            
            // Add warnings
            warnings.forEach(warning -> sb.append(warning).append('\n'));
            
            // Add info messages
            messages.forEach(message -> sb.append(message).append('\n'));
            

            return sb.toString();
        }
    }
}
