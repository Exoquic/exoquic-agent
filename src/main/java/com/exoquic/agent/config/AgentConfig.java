package com.exoquic.agent.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration class for the Exoquic PostgreSQL Agent.
 * Handles loading and validating configuration properties from a file or environment variables.
 */
public class AgentConfig {
    private static final Logger logger = LogManager.getLogger(AgentConfig.class);
    
    // Database connection
    private String dbHost = "localhost";
    private int dbPort = 5432;
    private String dbName;
    private String dbUser;
    private String dbPassword;
    private String dbSchema = "public";

    // Standard PostgreSQL environment variables
    private static final String[] PG_ENV_VARS = {
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD"
    };
    
    // Replication settings
    private String replicationSlotName = "exoquic_agent_slot";
    private String publicationName = "exoquic_agent_pub";
    
    // HTTP settings
    private String exoquicBaseUrl;
    private String apiKey;
    private int connectionTimeout = 5000;
    private int socketTimeout = 30000;
    
    // Retry settings
    private int maxRetries = 5;
    private long initialRetryDelayMs = 1000;
    private long maxRetryDelayMs = 60000;
    
    // Performance settings
    private int pollIntervalMs = 1000;
    private int batchSize = 100;
    
    /**
     * Default constructor with default values.
     */
    public AgentConfig() {
        // Default constructor with default values
    }
    
    /**
     * Loads configuration from a properties file.
     * 
     * @param configPath Path to the properties file
     * @throws RuntimeException if the configuration file cannot be loaded or is invalid
     */
    public void loadFromFile(String configPath) {
        Properties props = new Properties();
        
        try (InputStream input = new FileInputStream(configPath)) {
            props.load(input);
            
            // Check PostgreSQL environment variables first
            String pgHost = System.getenv("PGHOST");
            String pgPort = System.getenv("PGPORT");
            String pgDatabase = System.getenv("PGDATABASE");
            String pgUser = System.getenv("PGUSER");
            String pgPassword = System.getenv("PGPASSWORD");

            // Use environment variables if available, otherwise fall back to properties
            dbHost = pgHost != null ? pgHost : getProperty(props, "pg.host", "db.host", dbHost);
            dbPort = pgPort != null ? Integer.parseInt(pgPort) : getIntProperty(props, "pg.port", "db.port", dbPort);
            dbName = pgDatabase != null ? pgDatabase : getRequiredProperty(props, "pg.database", "db.name");
            dbUser = pgUser != null ? pgUser : getRequiredProperty(props, "pg.user", "db.user");
            dbPassword = pgPassword != null ? pgPassword : getRequiredProperty(props, "pg.password", "db.password");
            dbSchema = getProperty(props, "pg.schema", "db.schema", dbSchema);
            
            // Replication settings
            replicationSlotName = getProperty(props, "replication.slot.name", "replication.slot.name", replicationSlotName);
            publicationName = getProperty(props, "replication.publication.name", "replication.publication.name", publicationName);
            
            // HTTP settings
            exoquicBaseUrl = getRequiredProperty(props, "exoquic.baseurl", "exoquic.baseurl");
            apiKey = getRequiredProperty(props, "exoquic.api.key", "exoquic.api.key");
            connectionTimeout = getIntProperty(props, "http.connection.timeout", "http.connection.timeout", connectionTimeout);
            socketTimeout = getIntProperty(props, "http.socket.timeout", "http.socket.timeout", socketTimeout);
            
            // Retry settings
            maxRetries = getIntProperty(props, "retry.max", "retry.max", maxRetries);
            initialRetryDelayMs = getLongProperty(props, "retry.initial.delay.ms", "retry.initial.delay.ms", initialRetryDelayMs);
            maxRetryDelayMs = getLongProperty(props, "retry.max.delay.ms", "retry.max.delay.ms", maxRetryDelayMs);
            
            // Performance settings
            pollIntervalMs = getIntProperty(props, "poll.interval.ms", "poll.interval.ms", pollIntervalMs);
            batchSize = getIntProperty(props, "batch.size", "batch.size", batchSize);
            
            // Validate configuration
            validate();
            
            logger.info("Configuration loaded successfully from {}", configPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration from " + configPath, e);
        }
    }
    
    /**
     * Validates the configuration.
     * 
     * @throws IllegalArgumentException if the configuration is invalid
     */
    private void validate() {
        if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Database name is required");
        }
        if (dbUser == null || dbUser.isEmpty()) {
            throw new IllegalArgumentException("Database user is required");
        }
        if (dbPassword == null || dbPassword.isEmpty()) {
            throw new IllegalArgumentException("Database password is required");
        }
        if (exoquicBaseUrl == null || exoquicBaseUrl.isEmpty()) {
            throw new IllegalArgumentException("Exoquic base url is required");
        }
        if (apiKey == null || apiKey.isEmpty()) {
            throw new IllegalArgumentException("API key is required");
        }
        if (dbPort <= 0) {
            throw new IllegalArgumentException("Invalid database port: " + dbPort);
        }
        if (connectionTimeout <= 0) {
            throw new IllegalArgumentException("Invalid connection timeout: " + connectionTimeout);
        }
        if (socketTimeout <= 0) {
            throw new IllegalArgumentException("Invalid socket timeout: " + socketTimeout);
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Invalid max retries: " + maxRetries);
        }
        if (initialRetryDelayMs <= 0) {
            throw new IllegalArgumentException("Invalid initial retry delay: " + initialRetryDelayMs);
        }
        if (maxRetryDelayMs <= initialRetryDelayMs) {
            throw new IllegalArgumentException("Max retry delay must be greater than initial retry delay");
        }
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("Invalid poll interval: " + pollIntervalMs);
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
    }
    
    /**
     * Gets a required property from the properties object.
     * Checks both new (pg.*) and old (db.*) property formats.
     * 
     * @param props Properties object
     * @param pgKey New property key format (pg.*)
     * @param dbKey Old property key format (db.*)
     * @return Property value
     * @throws IllegalArgumentException if the property is not found
     */
    private String getRequiredProperty(Properties props, String pgKey, String dbKey) {
        // Try new format first
        String value = props.getProperty(pgKey);
        if (value == null || value.isEmpty()) {
            // Try old format
            value = props.getProperty(dbKey);
        }
        
        if (value == null || value.isEmpty()) {
            // Check environment variables
            String envKey = dbKey.replace('.', '_').toUpperCase();
            value = System.getenv(envKey);
            
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Required property not found: " + pgKey + " or " + dbKey);
            }
        }
        return value;
    }
    
    /**
     * Gets a property from the properties object with a default value.
     * Checks both new (pg.*) and old (db.*) property formats.
     * 
     * @param props Properties object
     * @param pgKey New property key format (pg.*)
     * @param dbKey Old property key format (db.*)
     * @param defaultValue Default value
     * @return Property value or default value if not found
     */
    private String getProperty(Properties props, String pgKey, String dbKey, String defaultValue) {
        // Try new format first
        String value = props.getProperty(pgKey);
        if (value == null || value.isEmpty()) {
            // Try old format
            value = props.getProperty(dbKey);
        }
        
        if (value == null || value.isEmpty()) {
            // Check environment variables
            String envKey = dbKey.replace('.', '_').toUpperCase();
            value = System.getenv(envKey);
            
            if (value == null || value.isEmpty()) {
                return defaultValue;
            }
        }
        return value;
    }
    
    /**
     * Gets an integer property from the properties object with a default value.
     * 
     * @param props Properties object
     * @param defaultValue Default value
     * @return Property value as integer or default value if not found
     * @throws NumberFormatException if the property value is not a valid integer
     */
    private int getIntProperty(Properties props, String pgKey, String dbKey, int defaultValue) {
        String value = getProperty(props, pgKey, dbKey, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for property " + pgKey + " or " + dbKey + ": " + value, e);
        }
    }
    
    /**
     * Gets a long property from the properties object with a default value.
     * 
     * @param props Properties object
     * @param defaultValue Default value
     * @return Property value as long or default value if not found
     * @throws NumberFormatException if the property value is not a valid long
     */
    private long getLongProperty(Properties props, String pgKey, String dbKey, long defaultValue) {
        String value = getProperty(props, pgKey, dbKey, String.valueOf(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for property " + pgKey + " or " + dbKey + ": " + value, e);
        }
    }
    
    // Getters
    
    public String getDbHost() {
        return dbHost;
    }
    
    public int getDbPort() {
        return dbPort;
    }
    
    public String getDbName() {
        return dbName;
    }
    
    public String getDbUser() {
        return dbUser;
    }
    
    public String getDbPassword() {
        return dbPassword;
    }
    
    public String getDbSchema() {
        return dbSchema;
    }
    
    public String getReplicationSlotName() {
        return replicationSlotName;
    }
    
    public String getPublicationName() {
        return publicationName;
    }
    
    public String getExoquicBaseUrl() {
        return exoquicBaseUrl;
    }
    
    public String getApiKey() {
        return apiKey;
    }
    
    public int getConnectionTimeout() {
        return connectionTimeout;
    }
    
    public int getSocketTimeout() {
        return socketTimeout;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public long getInitialRetryDelayMs() {
        return initialRetryDelayMs;
    }
    
    public long getMaxRetryDelayMs() {
        return maxRetryDelayMs;
    }
    
    public int getPollIntervalMs() {
        return pollIntervalMs;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
}
