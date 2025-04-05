package com.exoquic.agent;

import com.exoquic.agent.config.AgentConfig;
import com.exoquic.agent.debezium.ReactiveDebeziumEngine;
import com.exoquic.agent.debezium.ReactiveEventProcessor;
import com.exoquic.agent.http.ReactiveHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;

import java.util.concurrent.CountDownLatch;

public class ExoquicAgent {
    private static final Logger logger = LogManager.getLogger(ExoquicAgent.class);

    private final ReactiveDebeziumEngine debeziumEngine;
    private final Disposable subscription;
    
    /**
     * Creates a new ExoquicAgent using environment variables for configuration.
     */
    public ExoquicAgent() {
        logger.info("Loading configuration from environment variables");
        AgentConfig config = new AgentConfig();
        
        ReactiveHttpClient httpClient = new ReactiveHttpClient(config);
        ReactiveEventProcessor eventProcessor = new ReactiveEventProcessor(config, httpClient);
        this.debeziumEngine = new ReactiveDebeziumEngine(config);
        
        // Setup ChangeEvent handler
        this.subscription = debeziumEngine.getEventFlux()
            .doOnNext(event -> logger.debug("Received event in pipeline: {}", 
                event.value() != null ? event.value().substring(0, Math.min(100, event.value().length())) + "..." : "null"))
            .transform(eventProcessor::processEvents)
            .subscribe(
                v -> {}, // Events are handled in the processor
                error -> logger.error("Error in event processing pipeline", error),
                () -> logger.info("Event processing pipeline completed")
            );
        
        logger.info("ExoquicAgent initialized successfully");
    }
    
    /**
     * Starts the agent.
     * 
     * @throws RuntimeException if the agent fails to start
     */
    public void start() {
        logger.info("Starting Exoquic PostgreSQL Agent");
        
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        
        try {
            logger.info("Initializing Debezium engine");
            debeziumEngine.start();
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            logger.info("Exoquic PostgreSQL Agent started successfully");
        } catch (Exception e) {
            String errorMsg = "Failed to start Exoquic PostgreSQL Agent: " + e.getMessage();
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
    
    /**
     * Stops the agent.
     */
    public void stop() {
        logger.info("Stopping Exoquic PostgreSQL Agent");
        
        // Dispose subscription
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        
        // Stop components
        debeziumEngine.stop();
        
        logger.info("Exoquic PostgreSQL Agent stopped successfully");
    }
    
    /**
     * Main method.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            logger.info("Starting Exoquic PostgreSQL Agent using environment variables");
            
            try {
                // Start the agent
                ExoquicAgent agent = new ExoquicAgent();
                agent.start();
                
                // Keep the application running until interrupted
                CountDownLatch latch = new CountDownLatch(1);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Agent interrupted");
                }
            } catch (Exception e) {
                logger.error("Failed to start agent: {}", e.getMessage(), e);
                System.exit(1);
            }
        } catch (Exception e) {
            // Catch any unexpected exceptions during startup
            logger.error("Unexpected error during agent startup: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}
