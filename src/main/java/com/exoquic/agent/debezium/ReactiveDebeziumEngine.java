package com.exoquic.agent.debezium;

import com.exoquic.agent.config.AgentConfig;
import com.exoquic.agent.database.PostgresConfigValidator;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Builder;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Json;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReactiveDebeziumEngine {
    private static final Logger logger = LogManager.getLogger(ReactiveDebeziumEngine.class);
    
    private final AgentConfig config;
    private DebeziumEngine<?> engine;
    private final Sinks.Many<ChangeEvent<String, String>> eventSink;
    private final Flux<ChangeEvent<String, String>> eventFlux;
    private ExecutorService executor;
    
    /**
     * Creates a new ReactiveDebeziumEngine with the specified configuration.
     * 
     * @param config Agent configuration
     */
    public ReactiveDebeziumEngine(AgentConfig config) {
        this.config = config;
        this.eventSink = Sinks.many().multicast().onBackpressureBuffer();
        this.eventFlux = eventSink.asFlux();
        logger.info("ReactiveDebeziumEngine initialized");
    }
    
    /**
     * Gets the flux of change events.
     * 
     * @return Flux of change events
     */
    public Flux<ChangeEvent<String, String>> getEventFlux() {
        return eventFlux;
    }
    
    /**
     * Starts the Debezium engine.
     * 
     * @throws RuntimeException if the engine fails to start
     */
    public void start() {
        logger.info("Starting Debezium engine");
        Properties props = createDebeziumProperties();
        
        logger.info("Database connection properties: host={}, port={}, dbname={}, user={}",
            config.getDbHost(), config.getDbPort(), config.getDbName(), config.getDbUser());
        
        // Validating the postgres server and database to work with Exoquic. This checks for WAL settings
        // available replica slots, publications and more. If there are missing settings, it will automatically
        // configure it. The provided Postgres user is therefore expected to have the permissions required for
        // configuring WAL settings, creating replica slots, and more.
        PostgresConfigValidator validator = new PostgresConfigValidator(config);
        PostgresConfigValidator.ValidationResult validationResult = validator.validateAndConfigure();

        // Check if the postgres validation and configuration process is successful.
        logger.info("PostgreSQL configuration validation results:\n{}", validationResult);
        if (!validationResult.isSuccess()) {
            throw new RuntimeException("PostgreSQL configuration validation failed");
        }

        try {
            // Create and configure the engine with the Json format
            this.engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(this::handleChangeEvent)
                .build();
                
            // Start the engine in a separate thread with enhanced error handling
            this.executor = Executors.newSingleThreadExecutor();
            
            executor.execute(engine);
            logger.info("Debezium engine started successfully");
        } catch (Exception e) {
            logger.error("Failed to start Debezium engine", e);
            throw new RuntimeException("Failed to start Debezium engine", e);
        }
    }
    
    /**
     * Handles change events from Debezium.
     * 
     * @param records List of change events
     * @param committer Record committer
     */
    @SuppressWarnings("unchecked")
    private void handleChangeEvent(List<?> records, DebeziumEngine.RecordCommitter<?> committer) {
        DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> typedCommitter = 
            (DebeziumEngine.RecordCommitter<ChangeEvent<String, String>>) committer;
        
        for (Object record : records) {
            try {
                ChangeEvent<String, String> changeEvent = 
                    (ChangeEvent<String, String>) record;
                
                logger.debug("Received change event: key={}, value={}",
                    changeEvent.key(), changeEvent.value());
                
                Sinks.EmitResult result = eventSink.tryEmitNext(changeEvent);
                if (result.isFailure()) {
                    logger.warn("Failed to emit event: {}", result);
                }

                typedCommitter.markProcessed(changeEvent);
            } catch (Exception e) {
                logger.error("Error processing record", e);
            }
        }
        
        try {
            typedCommitter.markBatchFinished();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while marking batch finished", e);
        }
    }
    
    /**
     * Creates Debezium properties for PostgreSQL connector.
     * 
     * @return Properties for Debezium engine
     */
    private Properties createDebeziumProperties() {
        Properties props = new Properties();
        
        // Engine properties
        props.setProperty("name", "exoquic-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "./offsets.dat");
        props.setProperty("offset.flush.interval.ms", "100");

        // Connection properties
        props.setProperty("database.hostname", config.getDbHost());
        props.setProperty("database.port", String.valueOf(config.getDbPort()));
        props.setProperty("database.user", config.getDbUser());
        props.setProperty("database.password", config.getDbPassword());
        props.setProperty("database.dbname", config.getDbName());
        props.setProperty("database.server.name", "exoquic-server");
        
        // Replication properties
        props.setProperty("slot.name", config.getReplicationSlotName());
        props.setProperty("publication.name", config.getPublicationName());
        
        // Schema handling
        props.setProperty("schema.include.list", config.getDbSchema());
        
        // Performance tuning
        props.setProperty("max.batch.size", String.valueOf(config.getBatchSize()));
        props.setProperty("max.queue.size", String.valueOf(config.getBatchSize() * 5));
        props.setProperty("poll.interval.ms", String.valueOf(config.getPollIntervalMs()));

        // Added optimizations for low latency
        props.put("schema.refresh.mode", "columns_diff_exclude_unchanged_toast"); // Better performance for schema handling


        // Snapshot options
        props.setProperty("snapshot.mode", "initial");

        props.setProperty("topic.prefix", "topicprefix");
        props.setProperty("plugin.name", "pgoutput");

        return props;
    }
    
    /**
     * Stops the Debezium engine.
     */
    public void stop() {
        logger.info("Stopping Debezium engine");
        
        if (engine != null) {
            try {
                engine.close();
                eventSink.tryEmitComplete();
                
                if (executor != null) {
                    executor.shutdown();
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                }
                
                logger.info("Debezium engine stopped successfully");
            } catch (IOException | InterruptedException e) {
                logger.error("Error stopping Debezium engine", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
