package com.example.stateful.dbsync.batch;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DbSyncBatchTest {

    @Test
    void keepsLastMutationPerPidAndEntityIdForTState() {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(0, 1, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "AAA", new T("t-1", "AAA", "R1", false, 10, 0), null)),
                record(0, 2, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "AAA", new T("t-2", "AAA", "R2", false, 20, 0), null)),
                record(0, 3, event(DbSyncMutationType.DELETE_UNPROCESSED_T, "AAA", new T("t-1", "AAA", "R1", false, 10, 10), null)),
                record(0, 4, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "AAA", new T("t-2", "AAA", "R2", false, 20, 5), null))
        ));

        assertThat(batch.upsertUnprocessedT()).extracting(e -> e.t().id()).containsExactly("t-2");
        assertThat(batch.deleteUnprocessedT()).containsExactly(new DbSyncBatch.StateIdentity("AAA", "t-1"));
    }

    @Test
    void keepsLastMutationPerPidAndEntityIdForSState() {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(1, 1, event(DbSyncMutationType.UPSERT_UNPROCESSED_S, "AAA", null, new S("s-1", "AAA", 10, 0))),
                record(1, 2, event(DbSyncMutationType.UPSERT_UNPROCESSED_S, "AAA", null, new S("s-2", "AAA", 20, 0))),
                record(1, 3, event(DbSyncMutationType.DELETE_UNPROCESSED_S, "AAA", null, new S("s-1", "AAA", 10, 10))),
                record(1, 4, event(DbSyncMutationType.UPSERT_UNPROCESSED_S, "AAA", null, new S("s-2", "AAA", 20, 7)))
        ));

        assertThat(batch.upsertUnprocessedS()).extracting(e -> e.s().id()).containsExactly("s-2");
        assertThat(batch.deleteUnprocessedS()).containsExactly(new DbSyncBatch.StateIdentity("AAA", "s-1"));
    }

    private static ConsumerRecord<String, DbSyncEnvelope> record(int partition, long offset, DbSyncEnvelope event) {
        return new ConsumerRecord<>("db-sync-events", partition, offset, event.pid(), event);
    }

    private static DbSyncEnvelope event(DbSyncMutationType type, String pid, T t, S s) {
        return new DbSyncEnvelope(type, pid, t, s, null, "input-events", 0, 123L, 1700000000000L, 0, type + "-" + pid + "-0");
    }
}
