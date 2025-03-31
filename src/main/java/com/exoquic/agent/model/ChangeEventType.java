package com.exoquic.agent.model;

/**
 * Enum representing the types of change events.
 */
public enum ChangeEventType {
    /**
     * A record was created.
     */
    CREATED("created"),
    
    /**
     * A record was updated.
     */
    UPDATED("updated"),
    
    /**
     * A record was removed.
     */
    REMOVED("removed");
    
    private final String value;
    
    ChangeEventType(String value) {
        this.value = value;
    }
    
    /**
     * Gets the string value of the event type.
     * 
     * @return String value
     */
    public String getValue() {
        return value;
    }
}
