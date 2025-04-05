package com.exoquic.agent.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Configuration class for the Exoquic PostgreSQL Agent.
 * Handles loading and validating configuration from environment variables.
 */
public class AgentConfig {
    private static final Logger logger = LogManager.getLogger(AgentConfig.class);
    
    // Database connection
    private String dbHost;
    private int dbPort;
    private String dbName;
    private String dbUser;
    private String dbPassword;
    private String dbSchema;

    // Replication settings
    private String replicationSlotName;
    private String publicationName;
    
    // HTTP settings
    private String exoquicBaseUrl;
    private String apiKey;
    private int connectionTimeout;
    private int socketTimeout;
    
    // Retry settings
    private int maxRetries;
    private long initialRetryDelayMs;
    private long maxRetryDelayMs;
    
    // Performance settings
    private int pollIntervalMs;
    private int batchSize;
    
    /**
     * Constructor that loads configuration from environment variables.
     */
    public AgentConfig() {
        loadFromEnvironment();
        validate();
        logger.info("Configuration loaded successfully from environment variables");
    }
    
    /**
     * Loads configuration from environment variables.
     */
    private void loadFromEnvironment() {
        // Database connection settings
        dbHost = getRequiredEnv("PGHOST");
        dbPort = getEnvAsIntOrDefault("PGPORT", 5432);
        dbName = getRequiredEnv("PGDATABASE");
        dbUser = getRequiredEnv("PGUSER");
        dbPassword = getRequiredEnv("PGPASSWORD");
        dbSchema = getEnvOrDefault("PGSCHEMA", "public");
        
        // Replication settings
        replicationSlotName = getEnvOrDefault("REPLICATION_SLOT_NAME", "exoquic_agent_slot");
        publicationName = getEnvOrDefault("PUBLICATION_NAME", "exoquic_agent_pub");
        
        // HTTP settings
        exoquicBaseUrl = getEnvOrDefault("EXOQUIC_BASE_URL", "https://db.exoquic.com/");
        apiKey = getRequiredEnv("EXOQUIC_API_KEY");
        connectionTimeout = getEnvAsIntOrDefault("HTTP_CONNECTION_TIMEOUT", 5000);
        socketTimeout = getEnvAsIntOrDefault("HTTP_SOCKET_TIMEOUT", 30000);
        
        // Retry settings
        maxRetries = getEnvAsIntOrDefault("MAX_RETRIES", 5);
        initialRetryDelayMs = getEnvAsLongOrDefault("INITIAL_RETRY_DELAY_MS", 1000L);
        maxRetryDelayMs = getEnvAsLongOrDefault("MAX_RETRY_DELAY_MS", 60000L);
        
        // Performance settings
        pollIntervalMs = getEnvAsIntOrDefault("POLL_INTERVAL_MS", 20);
        batchSize = getEnvAsIntOrDefault("BATCH_SIZE", 64);
    }
    
    /**
     * Gets a required environment variable.
     * 
     * @param key Environment variable key
     * @return Environment variable value
     * @throws IllegalArgumentException if the environment variable is not found
     */
    private String getRequiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Required environment variable not found: " + key);
        }
        return value;
    }
    
    /**
     * Gets an environment variable with a default value.
     * 
     * @param key Environment variable key
     * @param defaultValue Default value
     * @return Environment variable value or default value if not found
     */
    private String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
    
    /**
     * Gets an integer environment variable with a default value.
     * 
     * @param key Environment variable key
     * @param defaultValue Default value
     * @return Environment variable value as integer or default value if not found
     * @throws NumberFormatException if the environment variable value is not a valid integer
     */
    private int getEnvAsIntOrDefault(String key, int defaultValue) {
        String value = System.getenv(key);
        try {
            return (value != null && !value.isEmpty()) ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for environment variable " + key + ": " + value, e);
        }
    }
    
    /**
     * Gets a long environment variable with a default value.
     * 
     * @param key Environment variable key
     * @param defaultValue Default value
     * @return Environment variable value as long or default value if not found
     * @throws NumberFormatException if the environment variable value is not a valid long
     */
    private long getEnvAsLongOrDefault(String key, long defaultValue) {
        String value = System.getenv(key);
        try {
            return (value != null && !value.isEmpty()) ? Long.parseLong(value) : defaultValue;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for environment variable " + key + ": " + value, e);
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
