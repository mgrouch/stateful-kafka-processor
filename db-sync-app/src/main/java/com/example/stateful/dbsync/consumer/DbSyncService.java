package com.example.stateful.dbsync.consumer;

import com.example.stateful.dbsync.batch.DbSyncBatch;
import com.example.stateful.dbsync.config.DbSyncSettings;
import com.example.stateful.dbsync.repository.DbSyncRepository;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class DbSyncService {

    private static final Logger log = LoggerFactory.getLogger(DbSyncService.class);

    private final DbSyncSettings settings;
    private final ObjectMapper objectMapper;
    private final DbSyncRepository repository;

    public DbSyncService(DbSyncSettings settings, ObjectMapper objectMapper, DbSyncRepository repository) {
        this.settings = settings;
        this.objectMapper = objectMapper;
        this.repository = repository;
    }

    public void runForever() {
        repository.initializeSchema();
        try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
            List<TopicPartition> assigned = assignAndSeek(consumer);
            if (assigned.isEmpty()) {
                throw new IllegalStateException("No partitions discovered for topic " + settings.topic());
            }

            Instant nextRefresh = Instant.now().plus(settings.partitionDiscoveryInterval());
            while (true) {
                if (Instant.now().isAfter(nextRefresh)) {
                    List<TopicPartition> refreshed = assignAndSeek(consumer);
                    if (!refreshed.equals(assigned)) {
                        assigned = refreshed;
                        log.info("Refreshed assignment: {}", assigned);
                    }
                    nextRefresh = Instant.now().plus(settings.partitionDiscoveryInterval());
                }

                ConsumerRecords<String, byte[]> polled = consumer.poll(settings.pollTimeout());
                if (polled.isEmpty()) {
                    continue;
                }

                List<ConsumerRecord<String, DbSyncEnvelope>> decoded = decode(polled, settings.maxBatchRecords());
                DbSyncBatch batch = DbSyncBatch.from(decoded);
                repository.applyBatch(settings.consumerGroup(), settings.topic(), batch);
                log.info("Committed db-sync batch: records={}, partitions={}", decoded.size(), batch.nextOffsetsByPartition().keySet());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("db-sync-app failed fast because batch transaction or decode failed", exception);
        }
    }

    List<TopicPartition> assignAndSeek(KafkaConsumer<String, byte[]> consumer) {
        List<TopicPartition> partitions = consumer.partitionsFor(settings.topic()).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .sorted((left, right) -> Integer.compare(left.partition(), right.partition()))
                .toList();

        if (partitions.isEmpty()) {
            return partitions;
        }

        consumer.assign(partitions);
        List<Integer> partitionIds = partitions.stream().map(TopicPartition::partition).toList();
        Map<TopicPartition, Long> persisted = repository.loadOffsets(settings.consumerGroup(), settings.topic(), partitionIds);
        for (TopicPartition partition : partitions) {
            Long offset = persisted.get(partition);
            if (offset == null) {
                consumer.seekToBeginning(List.of(partition));
            } else {
                consumer.seek(partition, offset);
            }
        }
        return partitions;
    }

    private List<ConsumerRecord<String, DbSyncEnvelope>> decode(ConsumerRecords<String, byte[]> polled, int maxBatchRecords) {
        List<ConsumerRecord<String, DbSyncEnvelope>> decoded = new ArrayList<>();
        for (ConsumerRecord<String, byte[]> record : polled) {
            if (decoded.size() >= maxBatchRecords) {
                break;
            }
            try {
                DbSyncEnvelope event = objectMapper.readValue(record.value(), DbSyncEnvelope.class);
                decoded.add(new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.key(), event));
            } catch (Exception exception) {
                throw new IllegalStateException("Malformed db-sync payload at " + record.topic() + "-" + record.partition() + "@" + record.offset(), exception);
            }
        }
        return decoded;
    }

    private KafkaConsumer<String, byte[]> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.consumerGroup());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, settings.clientId());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.maxPollRecords());
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, settings.securityProtocol());
        putIfPresent(properties, "ssl.truststore.location", settings.sslTruststoreLocation());
        putIfPresent(properties, "ssl.truststore.password", settings.sslTruststorePassword());
        putIfPresent(properties, "ssl.keystore.location", settings.sslKeystoreLocation());
        putIfPresent(properties, "ssl.keystore.password", settings.sslKeystorePassword());
        putIfPresent(properties, "ssl.key.password", settings.sslKeyPassword());

        return new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
    }

    private static void putIfPresent(Properties properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }
}
