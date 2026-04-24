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
        String applicationId,
        String inputTopic,
        String outputTopic,
        String dbSyncTopic,
        Path stateDir,
        long commitIntervalMs,
        long allocationLotterySeed
) {

    public ProcessorSettings {
        requireText(bootstrapServers, "bootstrapServers");
        requireText(applicationId, "applicationId");
        requireText(inputTopic, "inputTopic");
        requireText(outputTopic, "outputTopic");
        requireText(dbSyncTopic, "dbSyncTopic");
        Objects.requireNonNull(stateDir, "stateDir must not be null");
        if (commitIntervalMs <= 0) {
            throw new IllegalArgumentException("commitIntervalMs must be > 0");
        }
    }

    public static ProcessorSettings from(Environment environment) {
        return new ProcessorSettings(
                environment.getRequiredProperty("spring.kafka.bootstrap-servers"),
                environment.getRequiredProperty("app.application-id"),
                environment.getRequiredProperty("app.input-topic"),
                environment.getRequiredProperty("app.output-topic"),
                environment.getRequiredProperty("app.db-sync-topic"),
                Path.of(environment.getRequiredProperty("app.state-dir")),
                Long.parseLong(environment.getRequiredProperty("app.commit-interval-ms")),
                Long.parseLong(environment.getProperty("allocation.lottery.seed", "1357911"))
        );
    }

    public Properties toStreamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, applicationId + "-streams");
        return properties;
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
