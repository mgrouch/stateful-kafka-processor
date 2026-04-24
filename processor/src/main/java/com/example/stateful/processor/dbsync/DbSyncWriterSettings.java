package com.example.stateful.processor.dbsync;

import com.example.stateful.processor.config.ProcessorSettings;
import org.springframework.core.env.Environment;

import java.time.Duration;

public record DbSyncWriterSettings(
        boolean enabled,
        String bootstrapServers,
        String topic,
        String clientId,
        String dbUrl,
        String dbUser,
        String dbPassword,
        Duration pollTimeout,
        int maxBatchSize
) {

    public static DbSyncWriterSettings from(Environment environment, ProcessorSettings processorSettings) {
        boolean enabled = Boolean.parseBoolean(environment.getProperty("app.db-sync-writer.enabled", "false"));
        String dbUrl = environment.getProperty("app.db-sync-writer.db-url", "jdbc:h2:mem:dbsync;DB_CLOSE_DELAY=-1");
        String dbUser = environment.getProperty("app.db-sync-writer.db-user", "sa");
        String dbPassword = environment.getProperty("app.db-sync-writer.db-password", "");
        long pollMs = Long.parseLong(environment.getProperty("app.db-sync-writer.poll-ms", "500"));
        int maxBatchSize = Integer.parseInt(environment.getProperty("app.db-sync-writer.max-batch-size", "200"));

        return new DbSyncWriterSettings(
                enabled,
                processorSettings.bootstrapServers(),
                processorSettings.dbSyncTopic(),
                processorSettings.applicationId() + "-db-sync-writer",
                dbUrl,
                dbUser,
                dbPassword,
                Duration.ofMillis(pollMs),
                maxBatchSize
        );
    }
}
