package com.example.stateful.dbsync.repository;

import com.example.stateful.dbsync.batch.DbSyncBatch;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DbSyncRepositoryTest {

    private static final String TOPIC = "db-sync-events";
    private static final String GROUP = "db-sync-app";

    private DataSource dataSource;
    private DbSyncRepository repository;

    @BeforeEach
    void setup() {
        String dbName = "dbsync-app-test-" + System.nanoTime();
        dataSource = new DriverManagerDataSource("jdbc:h2:mem:" + dbName + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        repository = new DbSyncRepository(dataSource);
        repository.initializeSchema();
    }

    @Test
    void offsetsAndRowChangesCommitInSingleTransaction() throws Exception {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(0, 10, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-1", "IBM", "R", false, 11, 0), null, null, 0)),
                record(0, 11, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "IBM", new T("t-1", "IBM", "R", false, 11, 0), null, null, 1)),
                record(1, 3, event(DbSyncMutationType.ACCEPTED_S, "MSFT", null, new S("s-1", "MSFT", 9, 0), null, 2))
        ));

        repository.applyBatch(GROUP, TOPIC, batch);

        try (Connection connection = dataSource.getConnection()) {
            assertCount(connection, "SELECT COUNT(*) FROM accepted_t", 1);
            assertCount(connection, "SELECT COUNT(*) FROM accepted_s", 1);
            assertCount(connection, "SELECT COUNT(*) FROM unprocessed_t_state", 1);
            assertOffset(connection, 0, 12);
            assertOffset(connection, 1, 4);
        }
    }

    @Test
    void restartLoadsOffsetsFromDatabase() {
        DbSyncBatch batch = DbSyncBatch.from(List.of(record(2, 25, event(DbSyncMutationType.GENERATED_TS, "IBM", null, null, new TS("ts-1", "IBM", "t-1", "s-1", 99), 0))));
        repository.applyBatch(GROUP, TOPIC, batch);

        Map<TopicPartition, Long> offsets = repository.loadOffsets(GROUP, TOPIC, List.of(2));
        assertThat(offsets.get(new TopicPartition(TOPIC, 2))).isEqualTo(26L);
    }

    @Test
    void stateUpsertAndDeleteKeepFinalStateCorrect() throws Exception {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(0, 1, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "IBM", new T("t-1", "IBM", "R1", false, 1, 0), null, null, 0)),
                record(0, 2, event(DbSyncMutationType.DELETE_UNPROCESSED_T, "IBM", null, null, null, 1)),
                record(0, 3, event(DbSyncMutationType.UPSERT_UNPROCESSED_S, "IBM", null, new S("s-1", "IBM", 4, 0), null, 2))
        ));

        repository.applyBatch(GROUP, TOPIC, batch);

        try (Connection connection = dataSource.getConnection()) {
            assertCount(connection, "SELECT COUNT(*) FROM unprocessed_t_state", 0);
            assertCount(connection, "SELECT COUNT(*) FROM unprocessed_s_state", 1);
        }
    }

    @Test
    void rollbackFailureStopsBatchAndDoesNotAdvanceOffsets() {
        DataSource failingDataSource = commitFailingDataSource(dataSource);
        DbSyncRepository failingRepository = new DbSyncRepository(failingDataSource);

        DbSyncBatch batch = DbSyncBatch.from(List.of(record(0, 5, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-2", "IBM", "R2", false, 12, 0), null, null, 0))));

        assertThatThrownBy(() -> failingRepository.applyBatch(GROUP, TOPIC, batch))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed applying db-sync batch");

        Map<TopicPartition, Long> offsets = repository.loadOffsets(GROUP, TOPIC, List.of(0));
        assertThat(offsets).isEmpty();
    }

    @Test
    void acceptedAndGeneratedTablesAreWritten() throws Exception {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(0, 0, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-3", "IBM", "R3", false, 20, 0), null, null, 0)),
                record(0, 1, event(DbSyncMutationType.ACCEPTED_S, "IBM", null, new S("s-3", "IBM", 21, 0), null, 1)),
                record(0, 2, event(DbSyncMutationType.GENERATED_TS, "IBM", null, null, new TS("ts-3", "IBM", "t-3", "s-3", 22), 2))
        ));

        repository.applyBatch(GROUP, TOPIC, batch);

        try (Connection connection = dataSource.getConnection()) {
            assertCount(connection, "SELECT COUNT(*) FROM accepted_t", 1);
            assertCount(connection, "SELECT COUNT(*) FROM accepted_s", 1);
            assertCount(connection, "SELECT COUNT(*) FROM generated_ts", 1);
        }
    }

    @Test
    void batchingWithMultipleMutationTypesCommitsTogether() throws Exception {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(0, 40, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-40", "IBM", "R", false, 40, 0), null, null, 0)),
                record(0, 41, event(DbSyncMutationType.UPSERT_UNPROCESSED_T, "IBM", new T("t-40", "IBM", "R", false, 40, 0), null, null, 1)),
                record(0, 42, event(DbSyncMutationType.DELETE_UNPROCESSED_S, "IBM", null, null, null, 2)),
                record(1, 8, event(DbSyncMutationType.ACCEPTED_S, "MSFT", null, new S("s-8", "MSFT", 8, 0), null, 3))
        ));

        repository.applyBatch(GROUP, TOPIC, batch);

        try (Connection connection = dataSource.getConnection()) {
            assertCount(connection, "SELECT COUNT(*) FROM accepted_t", 1);
            assertCount(connection, "SELECT COUNT(*) FROM accepted_s", 1);
            assertOffset(connection, 0, 43);
            assertOffset(connection, 1, 9);
        }
    }

    @Test
    void partitionAwareOffsetsPersistMaxPerPartition() {
        DbSyncBatch batch = DbSyncBatch.from(List.of(
                record(0, 10, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-10", "IBM", "R", false, 10, 0), null, null, 0)),
                record(0, 12, event(DbSyncMutationType.ACCEPTED_T, "IBM", new T("t-12", "IBM", "R", false, 12, 0), null, null, 1)),
                record(1, 1, event(DbSyncMutationType.ACCEPTED_S, "MSFT", null, new S("s-1", "MSFT", 1, 0), null, 2))
        ));

        repository.applyBatch(GROUP, TOPIC, batch);

        Map<TopicPartition, Long> offsets = repository.loadOffsets(GROUP, TOPIC, List.of(0, 1));
        assertThat(offsets.get(new TopicPartition(TOPIC, 0))).isEqualTo(13L);
        assertThat(offsets.get(new TopicPartition(TOPIC, 1))).isEqualTo(2L);
    }

    private static ConsumerRecord<String, DbSyncEnvelope> record(int partition, long offset, DbSyncEnvelope event) {
        return new ConsumerRecord<>(TOPIC, partition, offset, event.pid(), event);
    }

    private static DbSyncEnvelope event(DbSyncMutationType type, String pid, T t, S s, TS ts, int ordinal) {
        return new DbSyncEnvelope(type, pid, t, s, ts, "input-events", 0, 123L, 1700000000000L, ordinal, type + "-" + pid + "-" + ordinal);
    }

    private static void assertCount(Connection connection, String sql, int expected) throws SQLException {
        try (var rs = connection.createStatement().executeQuery(sql)) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(expected);
        }
    }

    private static void assertOffset(Connection connection, int partition, long nextOffset) throws SQLException {
        try (var rs = connection.createStatement().executeQuery("SELECT next_offset FROM kafka_consumer_offsets WHERE consumer_group='" + GROUP + "' AND topic='" + TOPIC + "' AND partition_id=" + partition)) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(nextOffset);
        }
    }

    private static DataSource commitFailingDataSource(DataSource delegate) {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return wrap(delegate.getConnection());
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                return wrap(delegate.getConnection(username, password));
            }

            private Connection wrap(Connection connection) {
                return (Connection) Proxy.newProxyInstance(
                        Connection.class.getClassLoader(),
                        new Class[]{Connection.class},
                        (proxy, method, args) -> {
                            if ("commit".equals(method.getName())) {
                                throw new SQLException("forced commit failure");
                            }
                            return method.invoke(connection, args);
                        }
                );
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException { return delegate.unwrap(iface); }
            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException { return delegate.isWrapperFor(iface); }
            @Override
            public java.io.PrintWriter getLogWriter() throws SQLException { return delegate.getLogWriter(); }
            @Override
            public void setLogWriter(java.io.PrintWriter out) throws SQLException { delegate.setLogWriter(out); }
            @Override
            public void setLoginTimeout(int seconds) throws SQLException { delegate.setLoginTimeout(seconds); }
            @Override
            public int getLoginTimeout() throws SQLException { return delegate.getLoginTimeout(); }
            @Override
            public java.util.logging.Logger getParentLogger() { throw new UnsupportedOperationException(); }
        };
    }
}
