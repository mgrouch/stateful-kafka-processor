package com.example.stateful.dbsync.config;

import org.springframework.core.env.Environment;

import java.time.Duration;

public record DbSyncSettings(
        String bootstrapServers,
        String securityProtocol,
        String sslTruststoreLocation,
        String sslTruststorePassword,
        String sslKeystoreLocation,
        String sslKeystorePassword,
        String sslKeyPassword,
        String topic,
        String consumerGroup,
        String clientId,
        Duration pollTimeout,
        int maxPollRecords,
        int maxBatchRecords,
        Duration partitionDiscoveryInterval,
        String dbUrl,
        String dbUser,
        String dbPassword
) {

    public static DbSyncSettings from(Environment environment) {
        return new DbSyncSettings(
                required(environment, "app.bootstrap-servers"),
                environment.getProperty("app.kafka.security-protocol", "SSL"),
                environment.getProperty("app.kafka.ssl.truststore-location", ""),
                environment.getProperty("app.kafka.ssl.truststore-password", ""),
                environment.getProperty("app.kafka.ssl.keystore-location", ""),
                environment.getProperty("app.kafka.ssl.keystore-password", ""),
                environment.getProperty("app.kafka.ssl.key-password", ""),
                required(environment, "app.topic"),
                required(environment, "app.consumer-group"),
                required(environment, "app.client-id"),
                Duration.ofMillis(intProperty(environment, "app.poll-timeout-ms", 500)),
                intProperty(environment, "app.max-poll-records", 5000),
                intProperty(environment, "app.max-batch-records", 5000),
                Duration.ofMillis(intProperty(environment, "app.partition-discovery-interval-ms", 30000)),
                required(environment, "app.db.url"),
                required(environment, "app.db.user"),
                environment.getProperty("app.db.password", "")
        );
    }

    private static int intProperty(Environment environment, String key, int defaultValue) {
        Integer value = environment.getProperty(key, Integer.class, defaultValue);
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be > 0");
        }
        return value;
    }

    private static String required(Environment environment, String key) {
        String value = environment.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " must not be blank");
        }
        return value;
    }
}
