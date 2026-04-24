package com.example.stateful.processor;

import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DbSyncWriterTest {

    @Test
    void appliesAllMutationsForSingleSourceRecordInSingleTransactionAndStoresOffset() {
        JdbcDbSyncSqlRepository repository = repository();
        DbSyncWriter writer = writer(repository);

        TopicPartition partition = new TopicPartition("db-sync-events", 0);

        MockConsumer<String, DbSyncEnvelope> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions("db-sync-events", List.of(new PartitionInfo("db-sync-events", 0, null, null, null)));
        consumer.updateBeginningOffsets(java.util.Map.of(partition, 0L));
        writer.initialize(consumer);

        consumer.addRecord(new ConsumerRecord<>("db-sync-events", 0, 0, "IBM", dbSync(1, DbSyncMutationType.ACCEPTED_T)));
        consumer.addRecord(new ConsumerRecord<>("db-sync-events", 0, 1, "IBM", dbSync(2, DbSyncMutationType.UPSERT_UNPROCESSED_T)));
        consumer.addRecord(new ConsumerRecord<>("db-sync-events", 0, 2, "IBM", dbSync(3, DbSyncMutationType.GENERATED_TS)));

        writer.pollAndApplyOnce(consumer);

        assertThat(repository.loadOffsets("db-sync-writer", "db-sync-events")).containsEntry(0, 3L);
        assertThat(queryLong(repository, "select count(*) from accepted_t")).isEqualTo(1L);
        assertThat(queryLong(repository, "select count(*) from unprocessed_t_state")).isEqualTo(1L);
        assertThat(queryLong(repository, "select count(*) from generated_ts")).isEqualTo(1L);
        assertThat(consumer.position(partition)).isEqualTo(3L);
    }

    @Test
    void restartSeeksFromSqlOffset() {
        JdbcDbSyncSqlRepository repository = repository();
        DbSyncWriter writer = writer(repository);

        MockConsumer<String, DbSyncEnvelope> first = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        first.updatePartitions("db-sync-events", List.of(new PartitionInfo("db-sync-events", 0, null, null, null)));
        first.updateBeginningOffsets(java.util.Map.of(new TopicPartition("db-sync-events", 0), 0L));
        writer.initialize(first);
        first.addRecord(new ConsumerRecord<>("db-sync-events", 0, 0, "IBM", dbSync(1, DbSyncMutationType.ACCEPTED_T)));
        writer.pollAndApplyOnce(first);

        MockConsumer<String, DbSyncEnvelope> restarted = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        restarted.updatePartitions("db-sync-events", List.of(new PartitionInfo("db-sync-events", 0, null, null, null)));
        restarted.updateBeginningOffsets(java.util.Map.of(new TopicPartition("db-sync-events", 0), 0L));
        writer.initialize(restarted);

        assertThat(restarted.position(new TopicPartition("db-sync-events", 0))).isEqualTo(1L);
    }

    private static JdbcDbSyncSqlRepository repository() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:test" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        JdbcDbSyncSqlRepository repository = new JdbcDbSyncSqlRepository(dataSource);
        repository.initializeSchema();
        return repository;
    }

    private static DbSyncWriter writer(DbSyncSqlRepository repository) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        SerdeFactory serdeFactory = new SerdeFactory(mapper);
        DbSyncWriterSettings settings = new DbSyncWriterSettings(
                true,
                "dummy:9092",
                "db-sync-events",
                "db-sync-writer",
                Duration.ofMillis(10)
        );
        return new DbSyncWriter(settings, serdeFactory, repository);
    }

    private static DbSyncEnvelope dbSync(int ordinal, DbSyncMutationType mutationType) {
        return switch (mutationType) {
            case ACCEPTED_T, UPSERT_UNPROCESSED_T -> new DbSyncEnvelope(
                    "input-events-0-100-" + ordinal + "-" + mutationType,
                    mutationType,
                    "IBM",
                    "input-events",
                    0,
                    100,
                    1000,
                    ordinal,
                    new T("t-1", "IBM", "R-1", false, 42),
                    null,
                    null
            );
            case GENERATED_TS -> new DbSyncEnvelope(
                    "input-events-0-100-" + ordinal + "-" + mutationType,
                    mutationType,
                    "IBM",
                    "input-events",
                    0,
                    100,
                    1000,
                    ordinal,
                    null,
                    null,
                    new TS("ts-1", "IBM", 42)
            );
            default -> throw new IllegalArgumentException("Unsupported in test: " + mutationType);
        };
    }

    private static long queryLong(JdbcDbSyncSqlRepository repository, String sql) {
        try (var connection = repository.dataSource().getConnection();
             var statement = connection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
