package com.exoquic.agent.http;

import com.exoquic.agent.config.AgentConfig;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Reactive HTTP client for sending events to Exoquic .
 * Uses WebClient from Spring WebFlux for non-blocking HTTP communication.
 */
public class ReactiveHttpClient {
    private static final Logger logger = LogManager.getLogger(ReactiveHttpClient.class);
    
    private final WebClient webClient;
    private final Retry retry;
    
    /**
     * Creates a new ReactiveHttpClient with the specified configuration.
     * 
     * @param config Agent configuration
     */
    public ReactiveHttpClient(AgentConfig config) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectionTimeout())
                .responseTimeout(Duration.ofMillis(config.getSocketTimeout()))
                .secure(sslContextSpec -> {
                    try {
                        sslContextSpec.sslContext(SslContextBuilder.forClient().build());
                    } catch (SSLException e) {
                        throw new RuntimeException(e);
                    }
                });

        this.webClient = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl(config.getExoquicBaseUrl())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .defaultHeader("x-api-key", config.getApiKey())
                .build();
        
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getMaxRetries())
                .waitDuration(Duration.ofMillis(config.getInitialRetryDelayMs()))
                .retryExceptions(IOException.class, TimeoutException.class, WebClientResponseException.class)
                .retryOnResult(createRetryPredicate())
                .build();
        
        this.retry = Retry.of("exoquic-http-retry", retryConfig);
        
        logger.info("ReactiveHttpClient initialized with endpoint: {}", config.getExoquicBaseUrl());
    }
    
    /**
     * Creates a predicate to determine if a response should be retried.
     * 
     * @return Predicate for retry condition
     */
    private Predicate<Object> createRetryPredicate() {
        return response -> {
            if (response instanceof WebClientResponseException) {
                int statusCode = ((WebClientResponseException) response).getStatusCode().value();
                // Retry on server errors (5xx)
                return statusCode >= 500;
            }
            return false;
        };
    }
    
    /**
     * Sends an event to the Exoquic platform.
     * 
     * @param payload JSON payload to send
     * @param topicName Name of the topic to send the event to
     * @return Mono that completes when the event is sent
     */
    public Mono<Void> sendEvent(String payload, String topicName) {
        if (payload == null || payload.isEmpty()) {
            logger.warn("Attempted to send empty payload, skipping");
            return Mono.empty();
        }
        
        return webClient.post()
                .uri("/topics/{topicName}", topicName)
                .headers(httpHeaders -> httpHeaders.add("Content-Type", "application/vnd.kafka.json.v2+json"))
                .bodyValue(payload)
                .retrieve()
                .bodyToMono(Void.class)
                .doOnSubscribe(s -> logger.debug("Sending event to Exoquic topic {}: {}", topicName, payload))
                .doOnSuccess(v -> logger.debug("Event sent successfully to topic {}", topicName))
                .doOnError(e -> logger.error("Error sending event to Exoquic topic {}: {}", topicName, e.getMessage()))
                .transformDeferred(RetryOperator.of(retry))
                .onErrorResume(e -> {
                    logger.error("Failed to send event to topic {} after retries: {}", topicName, e.getMessage());
                    return Mono.empty();
                });
    }
}
