package com.example.stateful.processor.dbsync;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import com.example.stateful.processor.serde.SerdeFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DbSyncWriterTest {

    private DataSource dataSource;
    private DbSyncWriter writer;

    @BeforeEach
    void setup() {
        dataSource = new DriverManagerDataSource("jdbc:h2:mem:dbsync-test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        SerdeFactory serdeFactory = new SerdeFactory(new ObjectMapper().findAndRegisterModules());
        writer = new DbSyncWriter(
                new DbSyncWriterSettings(false, "localhost:9092", "db-sync-events", "db-writer", "jdbc:h2:mem:dbsync-test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", java.time.Duration.ofMillis(100), 100),
                serdeFactory,
                dataSource
        );
        writer.initializeSchema();
    }

    @Test
    void applyBatchCommitsFactsStateAndOffsetAtomically() throws Exception {
        List<ConsumerRecord<String, DbSyncEnvelope>> records = List.of(
                record(0, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-1", "IBM", "R-1", false, 10L), null, null, 0)),
                record(1, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "IBM", new T("t-1", "IBM", "R-1", false, 10L), null, null, 1)),
                record(2, event(DbSyncMutationType.GENERATED_TS, "IBM", null, null, new TS("ts-t-1", "IBM", 10L), 2))
        );

        writer.applyBatch(records);

        try (var connection = dataSource.getConnection();
             var tRs = connection.createStatement().executeQuery("SELECT count(*) FROM accepted_t");
             var tsRs = connection.createStatement().executeQuery("SELECT count(*) FROM generated_ts");
             var stateRs = connection.createStatement().executeQuery("SELECT t_id FROM unprocessed_t_state WHERE pid='IBM'");
             var offRs = connection.createStatement().executeQuery("SELECT next_offset FROM kafka_consumer_offsets WHERE topic='db-sync-events' AND partition_id=0")) {
            tRs.next();
            tsRs.next();
            stateRs.next();
            offRs.next();
            assertThat(tRs.getInt(1)).isEqualTo(1);
            assertThat(tsRs.getInt(1)).isEqualTo(1);
            assertThat(stateRs.getString(1)).isEqualTo("t-1");
            assertThat(offRs.getLong(1)).isEqualTo(3L);
        }
    }

    @Test
    void loadOffsetsReturnsPersistedCheckpointForRestart() {
        writer.applyBatch(List.of(record(5, event(DbSyncMutationType.ACCEPTED_S, "IBM", null, new S("s-1", "IBM", 7L), null, 0))));

        Map<TopicPartition, Long> offsets = writer.loadOffsets("db-sync-events", List.of(0));
        assertThat(offsets.get(new TopicPartition("db-sync-events", 0))).isEqualTo(6L);
    }

    private static ConsumerRecord<String, DbSyncEnvelope> record(long offset, DbSyncEnvelope event) {
        return new ConsumerRecord<>("db-sync-events", 0, offset, event.pid(), event);
    }

    private static DbSyncEnvelope event(DbSyncMutationType type, String pid, T t, S s, TS ts, int ordinal) {
        return new DbSyncEnvelope(type, pid, t, s, ts, "input-events", 0, 42L, 1700000000000L, ordinal, "id-" + type + "-" + ordinal);
    }
}
