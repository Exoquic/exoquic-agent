package com.exoquic.agent.debezium;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONArray;
import com.exoquic.agent.config.AgentConfig;
import com.exoquic.agent.http.ReactiveHttpClient;
import com.exoquic.agent.model.ChangeEventType;
import io.debezium.engine.ChangeEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * Processes Debezium change events and transforms them into the format expected by Exoquic.
 * Uses a reactive approach with Project Reactor.
 */
public class ReactiveEventProcessor {
    private static final Logger logger = LogManager.getLogger(ReactiveEventProcessor.class);
    
    private final ReactiveHttpClient httpClient;
    private final AgentConfig config;
    /**
     * Creates a new ReactiveEventProcessor with the specified configuration and HTTP client.
     * 
     * @param config Agent configuration
     * @param httpClient HTTP client for sending events
     */
    public ReactiveEventProcessor(AgentConfig config, ReactiveHttpClient httpClient) {
        this.config = config;
        this.httpClient = httpClient;
        logger.info("ReactiveEventProcessor initialized");
    }
    
    /**
     * Processes a flux of change events.
     * 
     * @param eventFlux Flux of change events from Debezium
     * @return Flux of processed events
     */
    public Flux<Void> processEvents(Flux<ChangeEvent<String, String>> eventFlux) {
        return eventFlux
                .filter(this::isValidEvent)
                .flatMap(event -> transformEvent(event)
                        .flatMap(eventPayload -> {
                            // The topic name is [database].[schema].[table name]
                            String topicName = getTopicName(event);

                            // Retrieve the primary key, which is used as the event key and the channel.
                            String primaryKey = extractPrimaryKey(event);

                            JSONObject recordObj = new JSONObject();
                            recordObj.put("key", primaryKey);

                            try {
                                recordObj.put("value", JSON.parse(eventPayload));
                            } catch (JSONException jsonException) {
                                logger.warn("Failed to json deserialize event payload. Continuing without json deserialization", jsonException);
                                recordObj.put("value", eventPayload);
                            }

                            // Add headers
                            JSONObject headerObj = new JSONObject();
                            headerObj.put("key", "channel");
                            headerObj.put("value", new String(Base64.getEncoder().encode(primaryKey.getBytes())));

                            JSONArray headersArray = new JSONArray();
                            headersArray.add(headerObj);
                            recordObj.put("headers", headersArray);

                            // Create the records array
                            JSONArray recordsArray = new JSONArray();
                            recordsArray.add(recordObj);

                            // Create the final payload
                            JSONObject finalPayload = new JSONObject();
                            finalPayload.put("records", recordsArray);

                            String payload = JSON.toJSONString(finalPayload);

                            return httpClient.sendEvent(payload, topicName);
                        })
                )
                .doOnNext(v -> logger.debug("Event processed successfully"))
                .doOnError(e -> logger.error("Error processing event", e));
    }
    
    /**
     * Checks if an event is valid for processing.
     * 
     * @param event Change event
     * @return true if the event is valid, false otherwise
     */
    private boolean isValidEvent(ChangeEvent<String, String> event) {
        if (event == null || event.value() == null) {
            logger.debug("Skipping null event or event with null value");
            return false;
        }
        
        try {
            JSONObject jsonValue = JSON.parseObject(event.value());
            
            // Check if this is a schema change event (DDL)
            if (jsonValue.containsKey("schema") && jsonValue.containsKey("payload")) {
                JSONObject payload = jsonValue.getJSONObject("payload");
                if (payload.containsKey("ddl")) {
                    logger.debug("Skipping DDL event");
                    return false;
                }
                
                if (payload.isEmpty()) {
                    logger.debug("Skipping event with empty payload");
                    return false;
                }
                
                return true;
            }
            
            logger.debug("Skipping event with invalid JSON structure");
            return false;
        } catch (Exception e) {
            logger.warn("Error checking event validity: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Extracts the topic name from the change event.
     * Format: [database name].[schema].[table]
     * 
     * @param event Change event
     * @return Topic name in the format [database name].[schema].[table]
     */
    private String getTopicName(ChangeEvent<String, String> event) {
        try {
            // Parse the JSON string
            JSONObject jsonValue = JSON.parseObject(event.value());
            JSONObject payload = jsonValue.getJSONObject("payload");
            JSONObject source = payload.getJSONObject("source");
            
            String dbName = source.getString("db");
            String schema = source.getString("schema");
            String table = source.getString("table");
            
            // Construct topic name in the format [database name].[schema].[table]
            String topicName = String.format("%s.%s.%s", dbName, schema, table);
            logger.debug("Extracted topic name: {}", topicName);
            return topicName;
        } catch (Exception e) {
            // If there's an error extracting the topic name, use the database name as the default
            logger.warn("Error extracting topic name from event: {}", e.getMessage(), e);
            return config.getDbName();
        }
    }
    
    /**
     * For composite keys, sort field names and concatenate values
     * 
     * @param payload JSONObject containing the primary key fields
     * @return Concatenated primary key values in sorted field order
     */
    private String formatCompositeKey(JSONObject payload) {
        // Get all key names and sort them
        List<String> sortedKeys = new ArrayList<>(payload.keySet());
        Collections.sort(sortedKeys);
        
        // Build the composite key by concatenating values in sorted key order
        StringBuilder sb = new StringBuilder();
        for (String key : sortedKeys) {
            if (sb.length() > 0) {
                sb.append(":");
            }
            sb.append(payload.get(key));
        }
        return sb.toString();
    }
    
    /**
     * Extracts the primary key from the event key.
     * 
     * @param event Change event
     * @return Primary key as a string
     */
    private String extractPrimaryKey(ChangeEvent<String, String> event) {
        try {
            if (event.key() == null) {
                logger.warn("Event key is null, cannot extract primary key");
                return "unknown-key";
            }
            
            // Parse the key JSON
            JSONObject keyJson = JSON.parseObject(event.key());
            
            // Extract the payload which contains the primary key values
            JSONObject payload = keyJson.getJSONObject("payload");
            if (payload == null || payload.isEmpty()) {
                logger.warn("Key payload is null or empty");
                return "unknown-key";
            }
            
            // For a single primary key, we can just return the first value
            // For composite keys, sort fields by name and concatenate values
            if (payload.size() == 1) {
                // Get the first (and only) value
                return String.valueOf(payload.values().iterator().next());
            } else {
                // For composite keys, sort and concatenate
                return formatCompositeKey(payload);
            }
        } catch (Exception e) {
            logger.warn("Error extracting primary key: {}", e.getMessage(), e);
            return "error-key";
        }
    }
    
    /**
     * Transforms a Debezium change event into the Exoquic database log format '{"type": "created|deleted|updated", "data": {...}}'
     * 
     * @param event Change event
     * @return Mono with the JSON payload
     */
    private Mono<String> transformEvent(ChangeEvent<String, String> event) {
        return Mono.fromCallable(() -> {
            try {
                // Parse the JSON string
                JSONObject jsonValue = JSON.parseObject(event.value());
                JSONObject payload = jsonValue.getJSONObject("payload");
                
                // Extract operation type and data
                String op = payload.getString("op");
                JSONObject after = payload.getJSONObject("after");
                JSONObject before = payload.getJSONObject("before");
                
                // Determine event type and data
                ChangeEventType type;
                JSONObject data;

                switch (op) {
                    case "c" -> { // Create
                        type = ChangeEventType.CREATED;
                        data = after;
                    }
                    case "u" -> { // Update
                        type = ChangeEventType.UPDATED;
                        data = after;
                    }
                    case "d" -> { // Delete
                        type = ChangeEventType.REMOVED;
                        data = before;
                    }
                    default -> {
                        // Skip other operations
                        logger.debug("Skipping unsupported operation: {}", op);
                        return null;
                    }
                }
                
                if (data == null || data.isEmpty()) {
                    logger.warn("Skipping event with null or empty data");
                    return null;
                }
                
                // Create the event payload
                JSONObject exoquicPayload = new JSONObject();
                exoquicPayload.put("type", type.getValue());
                exoquicPayload.put("data", data);
                
                String jsonPayload = JSON.toJSONString(exoquicPayload);
                logger.debug("Transformed event: {}", jsonPayload);
                return jsonPayload;
            } catch (Exception e) {
                logger.error("Error transforming event: {}", e.getMessage(), e);
                return null;
            }
        })
        .filter(Objects::nonNull);
    }
}
