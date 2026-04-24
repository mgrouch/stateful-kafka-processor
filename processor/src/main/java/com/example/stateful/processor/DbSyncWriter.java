package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class DbSyncWriter {

    private final DbSyncWriterSettings settings;
    private final SerdeFactory serdeFactory;
    private final DbSyncSqlRepository repository;

    public DbSyncWriter(DbSyncWriterSettings settings, SerdeFactory serdeFactory, DbSyncSqlRepository repository) {
        this.settings = settings;
        this.serdeFactory = serdeFactory;
        this.repository = repository;
    }

    public KafkaConsumer<String, DbSyncEnvelope> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.consumerGroup());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(properties, serdeFactory.stringSerde().deserializer(), serdeFactory.dbSyncEnvelopeSerde().deserializer());
    }

    public void initialize(Consumer<String, DbSyncEnvelope> consumer) {
        repository.initializeSchema();
        List<TopicPartition> partitions = consumer.partitionsFor(settings.topic()).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .sorted(Comparator.comparingInt(TopicPartition::partition))
                .toList();
        consumer.assign(partitions);

        Map<Integer, Long> persistedOffsets = repository.loadOffsets(settings.consumerGroup(), settings.topic());
        for (TopicPartition partition : partitions) {
            Long nextOffset = persistedOffsets.get(partition.partition());
            if (nextOffset != null) {
                consumer.seek(partition, nextOffset);
            } else {
                consumer.seekToBeginning(List.of(partition));
            }
        }
    }

    public void pollAndApplyOnce(Consumer<String, DbSyncEnvelope> consumer) {
        ConsumerRecords<String, DbSyncEnvelope> records = consumer.poll(settings.pollTimeout());
        if (records.isEmpty()) {
            return;
        }

        Map<TopicPartition, List<ConsumerRecord<String, DbSyncEnvelope>>> byPartition = new HashMap<>();
        for (ConsumerRecord<String, DbSyncEnvelope> record : records) {
            byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), ignored -> new ArrayList<>()).add(record);
        }

        for (Map.Entry<TopicPartition, List<ConsumerRecord<String, DbSyncEnvelope>>> entry : byPartition.entrySet()) {
            applyPartitionBatch(entry.getKey(), entry.getValue());
        }
    }

    private void applyPartitionBatch(TopicPartition partition, List<ConsumerRecord<String, DbSyncEnvelope>> records) {
        records.sort(Comparator.comparingLong(ConsumerRecord::offset));

        List<ConsumerRecord<String, DbSyncEnvelope>> currentGroup = new ArrayList<>();
        String groupKey = null;

        for (ConsumerRecord<String, DbSyncEnvelope> record : records) {
            String nextGroupKey = sourceGroupingKey(record.value());
            if (groupKey == null || groupKey.equals(nextGroupKey)) {
                currentGroup.add(record);
                groupKey = nextGroupKey;
                continue;
            }
            flushGroup(partition, currentGroup);
            currentGroup = new ArrayList<>();
            currentGroup.add(record);
            groupKey = nextGroupKey;
        }

        if (!currentGroup.isEmpty()) {
            flushGroup(partition, currentGroup);
        }
    }

    private void flushGroup(TopicPartition partition, List<ConsumerRecord<String, DbSyncEnvelope>> records) {
        List<DbSyncEnvelope> envelopes = records.stream()
                .map(ConsumerRecord::value)
                .sorted(Comparator.comparingInt(DbSyncEnvelope::eventOrdinal))
                .toList();
        long nextOffset = records.get(records.size() - 1).offset() + 1;
        repository.applyBatch(settings.consumerGroup(), partition.topic(), partition.partition(), nextOffset, envelopes);
    }

    private static String sourceGroupingKey(DbSyncEnvelope envelope) {
        return envelope.sourceTopic() + "|" + envelope.sourcePartition() + "|" + envelope.sourceOffset();
    }
}
