package com.example.stateful.processor.dbsync;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import com.example.stateful.processor.serde.SerdeFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DbSyncWriter implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DbSyncWriter.class);

    private final DbSyncWriterSettings settings;
    private final SerdeFactory serdeFactory;
    private final DataSource dataSource;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread worker;

    public DbSyncWriter(DbSyncWriterSettings settings, SerdeFactory serdeFactory, DataSource dataSource) {
        this.settings = settings;
        this.serdeFactory = serdeFactory;
        this.dataSource = dataSource;
    }

    @Override
    public void start() {
        if (!settings.enabled() || !running.compareAndSet(false, true)) {
            return;
        }

        initializeSchema();

        worker = new Thread(this::runLoop, "db-sync-writer");
        worker.setDaemon(true);
        worker.start();
        log.info("Started db-sync writer for topic={}", settings.topic());
    }

    private void runLoop() {
        try (KafkaConsumer<String, DbSyncEnvelope> consumer = createConsumer()) {
            TopicPartition partition = new TopicPartition(settings.topic(), 0);
            consumer.assign(List.of(partition));

            Map<TopicPartition, Long> startOffsets = loadOffsets(settings.topic(), List.of(partition.partition()));
            Long startOffset = startOffsets.get(partition);
            if (startOffset != null) {
                consumer.seek(partition, startOffset);
            } else {
                consumer.seekToBeginning(List.of(partition));
            }

            while (running.get()) {
                ConsumerRecords<String, DbSyncEnvelope> records = consumer.poll(settings.pollTimeout());
                if (!records.isEmpty()) {
                    applyBatch(records.records(partition));
                }
            }
        } catch (Exception exception) {
            log.error("DB sync writer loop failed", exception);
            running.set(false);
        }
    }

    KafkaConsumer<String, DbSyncEnvelope> createConsumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", settings.bootstrapServers());
        properties.put("client.id", settings.clientId());
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("isolation.level", "read_committed");
        properties.put("max.poll.records", settings.maxBatchSize());
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return new KafkaConsumer<>(
                properties,
                serdeFactory.stringSerde().deserializer(),
                serdeFactory.dbSyncEnvelopeSerde().deserializer()
        );
    }

    void initializeSchema() {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String sql : DbSyncSql.CREATE_SCHEMA.split(";\\n")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        } catch (SQLException sqlException) {
            throw new IllegalStateException("Failed to initialize DB sync schema", sqlException);
        }
    }

    void applyBatch(List<ConsumerRecord<String, DbSyncEnvelope>> records) {
        if (records.isEmpty()) {
            return;
        }

        List<ConsumerRecord<String, DbSyncEnvelope>> ordered = new ArrayList<>(records);
        ordered.sort(Comparator.comparingLong(ConsumerRecord::offset));

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                for (ConsumerRecord<String, DbSyncEnvelope> record : ordered) {
                    applyMutation(connection, record.value());
                }

                ConsumerRecord<String, DbSyncEnvelope> last = ordered.get(ordered.size() - 1);
                upsertOffset(connection, last.topic(), last.partition(), last.offset() + 1);
                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed applying db-sync batch", exception);
        }
    }

    private void applyMutation(Connection connection, DbSyncEnvelope event) throws SQLException {
        switch (event.mutationType()) {
            case ACCEPTED_T -> upsertAcceptedT(connection, event);
            case ACCEPTED_S -> upsertAcceptedS(connection, event);
            case GENERATED_TS -> upsertGeneratedTs(connection, event);
            case UPSERT_UNPROCESSED_T -> upsertUnprocessedT(connection, event);
            case UPSERT_UNPROCESSED_S -> upsertUnprocessedS(connection, event);
            case DELETE_UNPROCESSED_T -> deleteUnprocessedT(connection, event.pid());
            case DELETE_UNPROCESSED_S -> deleteUnprocessedS(connection, event.pid());
        }
    }

    private void upsertAcceptedT(Connection connection, DbSyncEnvelope event) throws SQLException {
        String sql = """
                MERGE INTO accepted_t (id, pid, ref, cancel, q, q_a, source_topic, source_partition, source_offset, source_timestamp, event_id)
                KEY (id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.t().id());
            statement.setString(2, event.pid());
            statement.setString(3, event.t().ref());
            statement.setBoolean(4, event.t().cancel());
            statement.setLong(5, event.t().q());
            statement.setLong(6, event.t().q_a());
            setSourceColumns(statement, event, 7);
            statement.executeUpdate();
        }
    }

    private void upsertAcceptedS(Connection connection, DbSyncEnvelope event) throws SQLException {
        String sql = """
                MERGE INTO accepted_s (id, pid, q, q_a, source_topic, source_partition, source_offset, source_timestamp, event_id)
                KEY (id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.s().id());
            statement.setString(2, event.pid());
            statement.setLong(3, event.s().q());
            statement.setLong(4, event.s().q_a());
            setSourceColumns(statement, event, 5);
            statement.executeUpdate();
        }
    }

    private void upsertGeneratedTs(Connection connection, DbSyncEnvelope event) throws SQLException {
        String sql = """
                MERGE INTO generated_ts (id, pid, tid, sid, q_a, source_topic, source_partition, source_offset, source_timestamp, event_id)
                KEY (id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.ts().id());
            statement.setString(2, event.pid());
            statement.setString(3, event.ts().tid());
            statement.setString(4, event.ts().sid());
            statement.setLong(5, event.ts().q_a());
            setSourceColumns(statement, event, 6);
            statement.executeUpdate();
        }
    }

    private void upsertUnprocessedT(Connection connection, DbSyncEnvelope event) throws SQLException {
        String sql = """
                MERGE INTO unprocessed_t_state (pid, t_id, ref, cancel, q, q_a, source_topic, source_partition, source_offset, source_timestamp, event_id)
                KEY (pid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.pid());
            statement.setString(2, event.t().id());
            statement.setString(3, event.t().ref());
            statement.setBoolean(4, event.t().cancel());
            statement.setLong(5, event.t().q());
            statement.setLong(6, event.t().q_a());
            setSourceColumns(statement, event, 7);
            statement.executeUpdate();
        }
    }

    private void upsertUnprocessedS(Connection connection, DbSyncEnvelope event) throws SQLException {
        String sql = """
                MERGE INTO unprocessed_s_state (pid, s_id, q, q_a, source_topic, source_partition, source_offset, source_timestamp, event_id)
                KEY (pid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.pid());
            statement.setString(2, event.s().id());
            statement.setLong(3, event.s().q());
            statement.setLong(4, event.s().q_a());
            setSourceColumns(statement, event, 5);
            statement.executeUpdate();
        }
    }

    private void deleteUnprocessedT(Connection connection, String pid) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM unprocessed_t_state WHERE pid = ?")) {
            statement.setString(1, pid);
            statement.executeUpdate();
        }
    }

    private void deleteUnprocessedS(Connection connection, String pid) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM unprocessed_s_state WHERE pid = ?")) {
            statement.setString(1, pid);
            statement.executeUpdate();
        }
    }

    private static void setSourceColumns(PreparedStatement statement, DbSyncEnvelope event, int startIndex) throws SQLException {
        statement.setString(startIndex, event.sourceTopic());
        statement.setInt(startIndex + 1, event.sourcePartition());
        statement.setLong(startIndex + 2, event.sourceOffset());
        statement.setLong(startIndex + 3, event.sourceTimestamp());
        statement.setString(startIndex + 4, event.eventId());
    }

    private void upsertOffset(Connection connection, String topic, int partition, long nextOffset) throws SQLException {
        String sql = """
                MERGE INTO kafka_consumer_offsets (topic, partition_id, next_offset, updated_at)
                KEY (topic, partition_id)
                VALUES (?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, topic);
            statement.setInt(2, partition);
            statement.setLong(3, nextOffset);
            statement.setObject(4, Instant.now());
            statement.executeUpdate();
        }
    }

    Map<TopicPartition, Long> loadOffsets(String topic, Collection<Integer> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        String placeholders = String.join(",", partitions.stream().map(value -> "?").toList());
        String sql = "SELECT partition_id, next_offset FROM kafka_consumer_offsets WHERE topic = ? AND partition_id IN (" + placeholders + ")";
        Map<TopicPartition, Long> result = new HashMap<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, topic);
            int index = 2;
            for (Integer partition : partitions) {
                statement.setInt(index++, partition);
            }

            try (var rs = statement.executeQuery()) {
                while (rs.next()) {
                    int partition = rs.getInt(1);
                    long offset = rs.getLong(2);
                    result.put(new TopicPartition(topic, partition), offset);
                }
            }
            return result;
        } catch (SQLException sqlException) {
            throw new IllegalStateException("Failed loading db offsets", sqlException);
        }
    }

    @Override
    public void stop() {
        stop(() -> { });
    }

    @Override
    public void stop(Runnable callback) {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return settings.enabled();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}
