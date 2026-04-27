package com.example.stateful.processor.config;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessorSettingsTest {

    @Test
    void defaultsUseProductionOrientedReplicationAndStandbyValues() {
        MockEnvironment environment = baseEnvironment();

        ProcessorSettings settings = ProcessorSettings.from(environment);
        Properties properties = settings.toStreamsProperties();

        assertThat(settings.replicationFactor()).isEqualTo(ProcessorSettings.DEFAULT_REPLICATION_FACTOR);
        assertThat(settings.numStandbyReplicas()).isEqualTo(ProcessorSettings.DEFAULT_NUM_STANDBY_REPLICAS);
        assertThat(properties.get(StreamsConfig.REPLICATION_FACTOR_CONFIG)).isEqualTo(ProcessorSettings.DEFAULT_REPLICATION_FACTOR);
        assertThat(properties.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG)).isEqualTo(ProcessorSettings.DEFAULT_NUM_STANDBY_REPLICAS);
    }

    @Test
    void replicationAndStandbyAreOverridableFromApplicationProperties() {
        MockEnvironment environment = baseEnvironment()
                .withProperty(ProcessorSettings.REPLICATION_FACTOR_PROPERTY, "2")
                .withProperty(ProcessorSettings.NUM_STANDBY_REPLICAS_PROPERTY, "3");

        ProcessorSettings settings = ProcessorSettings.from(environment);
        Properties properties = settings.toStreamsProperties();

        assertThat(settings.replicationFactor()).isEqualTo(2);
        assertThat(settings.numStandbyReplicas()).isEqualTo(3);
        assertThat(properties.get(StreamsConfig.REPLICATION_FACTOR_CONFIG)).isEqualTo(2);
        assertThat(properties.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG)).isEqualTo(3);
    }

    private static MockEnvironment baseEnvironment() {
        return new MockEnvironment()
                .withProperty("spring.kafka.bootstrap-servers", "localhost:9092")
                .withProperty("app.kafka.security-protocol", "PLAINTEXT")
                .withProperty("app.application-id", "processor-test-app")
                .withProperty("app.input-topic", "input-topic")
                .withProperty("app.output-topic", "output-topic")
                .withProperty("app.db-sync-topic", "db-sync-topic")
                .withProperty("app.failed-t-topic", "failed-t-topic")
                .withProperty("app.s-with-q-carry-topic", "s-with-q-carry-topic")
                .withProperty("app.recon-report-topic", "recon-report-topic")
                .withProperty("app.state-dir", "build/tmp/kafka-state")
                .withProperty("app.commit-interval-ms", "100");
    }
}
