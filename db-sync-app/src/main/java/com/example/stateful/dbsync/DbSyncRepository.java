package com.example.stateful.dbsync;

import com.example.stateful.messaging.DbSyncEnvelope;
import org.apache.kafka.common.TopicPartition;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DbSyncRepository {

    private final DataSource dataSource;

    public DbSyncRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void initializeSchema() {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String sql : SchemaSql.createSchema(connection).split(";\\n")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to initialize db-sync schema", exception);
        }
    }

    public void applyBatch(String consumerGroup, String topic, DbSyncBatch batch) {
        if (batch.nextOffsetsByPartition().isEmpty()) {
            return;
        }

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                TempTableSql sql = TempTableSql.forConnection(connection);
                createTempTables(connection, sql);
                loadTempTables(connection, batch, sql);
                mergeFacts(connection, sql);
                mergeState(connection, sql);
                applyDeletes(connection, sql);
                upsertOffsets(connection, consumerGroup, topic, batch.nextOffsetsByPartition(), sql);
                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed applying db-sync batch. Service must be restarted after fixing the cause.", exception);
        }
    }

    public Map<TopicPartition, Long> loadOffsets(String consumerGroup, String topic, Collection<Integer> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        String placeholders = String.join(",", partitions.stream().map(value -> "?").toList());
        String sql = "SELECT partition_id, next_offset FROM kafka_consumer_offsets WHERE consumer_group = ? AND topic = ? AND partition_id IN (" + placeholders + ")";
        Map<TopicPartition, Long> result = new HashMap<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, consumerGroup);
            statement.setString(2, topic);
            int index = 3;
            for (Integer partition : partitions) {
                statement.setInt(index++, partition);
            }

            try (var rs = statement.executeQuery()) {
                while (rs.next()) {
                    int partition = rs.getInt(1);
                    long nextOffset = rs.getLong(2);
                    result.put(new TopicPartition(topic, partition), nextOffset);
                }
            }
            return result;
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed loading persisted offsets", exception);
        }
    }

    private static void createTempTables(Connection connection, TempTableSql sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql.createAcceptedTTemp());
            statement.execute(sql.createAcceptedSTemp());
            statement.execute(sql.createGeneratedTsTemp());
            statement.execute(sql.createUpsertUnprocessedTTemp());
            statement.execute(sql.createUpsertUnprocessedSTemp());
            statement.execute(sql.createDeleteUnprocessedTTemp());
            statement.execute(sql.createDeleteUnprocessedSTemp());
            statement.execute(sql.createOffsetsTemp());
        }
    }

    private static void loadTempTables(Connection connection, DbSyncBatch batch, TempTableSql sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql.insertAcceptedTTemp())) {
            for (DbSyncEnvelope event : batch.acceptedT()) {
                statement.setString(1, event.t().id());
                statement.setString(2, event.pid());
                statement.setString(3, event.t().ref());
                statement.setBoolean(4, event.t().cancel());
                statement.setLong(5, event.t().q());
                setSource(statement, event, 6);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertAcceptedSTemp())) {
            for (DbSyncEnvelope event : batch.acceptedS()) {
                statement.setString(1, event.s().id());
                statement.setString(2, event.pid());
                statement.setLong(3, event.s().q());
                setSource(statement, event, 4);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertGeneratedTsTemp())) {
            for (DbSyncEnvelope event : batch.generatedTs()) {
                statement.setString(1, event.ts().id());
                statement.setString(2, event.pid());
                statement.setLong(3, event.ts().q());
                setSource(statement, event, 4);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertUpsertUnprocessedTTemp())) {
            for (DbSyncEnvelope event : batch.upsertUnprocessedT()) {
                statement.setString(1, event.pid());
                statement.setString(2, event.t().id());
                statement.setString(3, event.t().ref());
                statement.setBoolean(4, event.t().cancel());
                statement.setLong(5, event.t().q());
                setSource(statement, event, 6);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertUpsertUnprocessedSTemp())) {
            for (DbSyncEnvelope event : batch.upsertUnprocessedS()) {
                statement.setString(1, event.pid());
                statement.setString(2, event.s().id());
                statement.setLong(3, event.s().q());
                setSource(statement, event, 4);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertDeleteUnprocessedTTemp())) {
            for (String pid : batch.deleteUnprocessedTPid()) {
                statement.setString(1, pid);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertDeleteUnprocessedSTemp())) {
            for (String pid : batch.deleteUnprocessedSPid()) {
                statement.setString(1, pid);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static void mergeFacts(Connection connection, TempTableSql sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql.mergeAcceptedT());
            statement.executeUpdate(sql.mergeAcceptedS());
            statement.executeUpdate(sql.mergeGeneratedTs());
        }
    }

    private static void mergeState(Connection connection, TempTableSql sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql.mergeUnprocessedT());
            statement.executeUpdate(sql.mergeUnprocessedS());
        }
    }

    private static void applyDeletes(Connection connection, TempTableSql sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql.deleteUnprocessedT());
            statement.executeUpdate(sql.deleteUnprocessedS());
        }
    }

    private static void upsertOffsets(Connection connection,
                                      String consumerGroup,
                                      String topic,
                                      Map<Integer, Long> nextOffsetsByPartition,
                                      TempTableSql sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql.insertOffsetsTemp())) {
            for (Map.Entry<Integer, Long> entry : nextOffsetsByPartition.entrySet()) {
                statement.setString(1, consumerGroup);
                statement.setString(2, topic);
                statement.setInt(3, entry.getKey());
                statement.setLong(4, entry.getValue());
                statement.setTimestamp(5, Timestamp.from(Instant.now()));
                statement.addBatch();
            }
            statement.executeBatch();
        }

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql.mergeOffsets());
        }
    }

    private static void setSource(PreparedStatement statement, DbSyncEnvelope event, int startIndex) throws SQLException {
        statement.setString(startIndex, event.sourceTopic());
        statement.setInt(startIndex + 1, event.sourcePartition());
        statement.setLong(startIndex + 2, event.sourceOffset());
        statement.setLong(startIndex + 3, event.sourceTimestamp());
        statement.setString(startIndex + 4, event.eventId());
    }
}
