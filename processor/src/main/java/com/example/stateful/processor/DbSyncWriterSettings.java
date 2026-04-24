package com.example.stateful.processor;

import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.Objects;

public record DbSyncWriterSettings(
        boolean enabled,
        String bootstrapServers,
        String topic,
        String consumerGroup,
        Duration pollTimeout
) {

    public DbSyncWriterSettings {
        requireText(bootstrapServers, "bootstrapServers");
        requireText(topic, "topic");
        requireText(consumerGroup, "consumerGroup");
        Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
    }

    public static DbSyncWriterSettings from(Environment env, ProcessorSettings processorSettings) {
        return new DbSyncWriterSettings(
                Boolean.parseBoolean(env.getProperty("app.db-writer.enabled", "false")),
                processorSettings.bootstrapServers(),
                processorSettings.dbSyncTopic(),
                env.getProperty("app.db-writer.consumer-group", "db-sync-writer"),
                Duration.ofMillis(Long.parseLong(env.getProperty("app.db-writer.poll-timeout-ms", "500")))
        );
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
