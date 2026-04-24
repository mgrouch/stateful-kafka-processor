package com.example.stateful.dbsync.batch;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record DbSyncBatch(
        List<DbSyncEnvelope> acceptedT,
        List<DbSyncEnvelope> acceptedS,
        List<DbSyncEnvelope> generatedTs,
        List<DbSyncEnvelope> upsertUnprocessedT,
        List<DbSyncEnvelope> upsertUnprocessedS,
        List<String> deleteUnprocessedTPid,
        List<String> deleteUnprocessedSPid,
        Map<Integer, Long> nextOffsetsByPartition
) {

    public static DbSyncBatch from(List<ConsumerRecord<String, DbSyncEnvelope>> records) {
        List<DbSyncEnvelope> acceptedT = new ArrayList<>();
        List<DbSyncEnvelope> acceptedS = new ArrayList<>();
        List<DbSyncEnvelope> generatedTs = new ArrayList<>();

        Map<String, OrderedMutation> tState = new HashMap<>();
        Map<String, OrderedMutation> sState = new HashMap<>();
        Map<Integer, Long> nextOffsets = new HashMap<>();

        for (ConsumerRecord<String, DbSyncEnvelope> record : records) {
            DbSyncEnvelope event = record.value();
            if (event == null) {
                throw new IllegalArgumentException("Received null db-sync event at offset " + record.offset());
            }

            nextOffsets.merge(record.partition(), record.offset() + 1, Math::max);

            switch (event.mutationType()) {
                case ACCEPTED_T -> acceptedT.add(event);
                case ACCEPTED_S -> acceptedS.add(event);
                case GENERATED_TS -> generatedTs.add(event);
                case UPSERT_UNPROCESSED_T, DELETE_UNPROCESSED_T -> tState.put(event.pid(), new OrderedMutation(record.partition(), record.offset(), event));
                case UPSERT_UNPROCESSED_S, DELETE_UNPROCESSED_S -> sState.put(event.pid(), new OrderedMutation(record.partition(), record.offset(), event));
            }
        }

        Comparator<OrderedMutation> orderComparator = Comparator
                .comparingInt(OrderedMutation::partition)
                .thenComparingLong(OrderedMutation::offset);

        List<DbSyncEnvelope> upsertUnprocessedT = tState.values().stream()
                .filter(mutation -> mutation.event().mutationType() == DbSyncMutationType.UPSERT_UNPROCESSED_T)
                .sorted(orderComparator)
                .map(OrderedMutation::event)
                .toList();
        List<String> deleteUnprocessedTPid = tState.values().stream()
                .filter(mutation -> mutation.event().mutationType() == DbSyncMutationType.DELETE_UNPROCESSED_T)
                .sorted(orderComparator)
                .map(mutation -> mutation.event().pid())
                .toList();

        List<DbSyncEnvelope> upsertUnprocessedS = sState.values().stream()
                .filter(mutation -> mutation.event().mutationType() == DbSyncMutationType.UPSERT_UNPROCESSED_S)
                .sorted(orderComparator)
                .map(OrderedMutation::event)
                .toList();
        List<String> deleteUnprocessedSPid = sState.values().stream()
                .filter(mutation -> mutation.event().mutationType() == DbSyncMutationType.DELETE_UNPROCESSED_S)
                .sorted(orderComparator)
                .map(mutation -> mutation.event().pid())
                .toList();

        return new DbSyncBatch(
                acceptedT,
                acceptedS,
                generatedTs,
                upsertUnprocessedT,
                upsertUnprocessedS,
                deleteUnprocessedTPid,
                deleteUnprocessedSPid,
                Map.copyOf(nextOffsets)
        );
    }

    private record OrderedMutation(int partition, long offset, DbSyncEnvelope event) {
    }
}
