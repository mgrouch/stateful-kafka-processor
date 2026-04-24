package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;

import java.util.Map;

public interface DbSyncSqlRepository {

    void initializeSchema();

    Map<Integer, Long> loadOffsets(String consumerGroup, String topic);

    void applyBatch(String consumerGroup, String topic, int partition, long nextOffset, java.util.List<DbSyncEnvelope> envelopes);
}
