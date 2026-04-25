package com.example.stateful.processor.config;

import org.springframework.core.env.Environment;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

public record ProcessorSettings(
        String bootstrapServers,
        String securityProtocol,
        String sslTruststoreLocation,
        String sslTruststorePassword,
        String sslKeystoreLocation,
        String sslKeystorePassword,
        String sslKeyPassword,
        String applicationId,
        String inputTopic,
        String outputTopic,
        String dbSyncTopic,
        Path stateDir,
        long commitIntervalMs,
        int replicationFactor,
        int numStandbyReplicas,
        long allocationLotterySeed
) {

    public static final String REPLICATION_FACTOR_PROPERTY = "app.streams.replication-factor";
    public static final String NUM_STANDBY_REPLICAS_PROPERTY = "app.streams.num-standby-replicas";
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    public static final int DEFAULT_NUM_STANDBY_REPLICAS = 1;

    public ProcessorSettings {
        requireText(bootstrapServers, "bootstrapServers");
        requireText(securityProtocol, "securityProtocol");
        requireText(applicationId, "applicationId");
        requireText(inputTopic, "inputTopic");
        requireText(outputTopic, "outputTopic");
        requireText(dbSyncTopic, "dbSyncTopic");
        Objects.requireNonNull(stateDir, "stateDir must not be null");
        if (commitIntervalMs <= 0) {
            throw new IllegalArgumentException("commitIntervalMs must be > 0");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be > 0");
        }
        if (numStandbyReplicas < 0) {
            throw new IllegalArgumentException("numStandbyReplicas must be >= 0");
        }
    }

    public static ProcessorSettings from(Environment environment) {
        return new ProcessorSettings(
                environment.getRequiredProperty("spring.kafka.bootstrap-servers"),
                environment.getProperty("app.kafka.security-protocol", "SSL"),
                environment.getProperty("app.kafka.ssl.truststore-location", ""),
                environment.getProperty("app.kafka.ssl.truststore-password", ""),
                environment.getProperty("app.kafka.ssl.keystore-location", ""),
                environment.getProperty("app.kafka.ssl.keystore-password", ""),
                environment.getProperty("app.kafka.ssl.key-password", ""),
                environment.getRequiredProperty("app.application-id"),
                environment.getRequiredProperty("app.input-topic"),
                environment.getRequiredProperty("app.output-topic"),
                environment.getRequiredProperty("app.db-sync-topic"),
                Path.of(environment.getRequiredProperty("app.state-dir")),
                Long.parseLong(environment.getRequiredProperty("app.commit-interval-ms")),
                Integer.parseInt(environment.getProperty(REPLICATION_FACTOR_PROPERTY, String.valueOf(DEFAULT_REPLICATION_FACTOR))),
                Integer.parseInt(environment.getProperty(NUM_STANDBY_REPLICAS_PROPERTY, String.valueOf(DEFAULT_NUM_STANDBY_REPLICAS))),
                Long.parseLong(environment.getProperty("allocation.lottery.seed", "1357911"))
        );
    }

    public Properties toStreamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
        properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, applicationId + "-streams");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        putIfPresent(properties, "ssl.truststore.location", sslTruststoreLocation);
        putIfPresent(properties, "ssl.truststore.password", sslTruststorePassword);
        putIfPresent(properties, "ssl.keystore.location", sslKeystoreLocation);
        putIfPresent(properties, "ssl.keystore.password", sslKeystorePassword);
        putIfPresent(properties, "ssl.key.password", sslKeyPassword);
        return properties;
    }

    private static void putIfPresent(Properties properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
