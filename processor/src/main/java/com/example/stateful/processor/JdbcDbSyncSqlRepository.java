package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JdbcDbSyncSqlRepository implements DbSyncSqlRepository {

    private final DataSource dataSource;

    public JdbcDbSyncSqlRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void initializeSchema() {
        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().executeUpdate("""
                    create table if not exists accepted_t (
                      id varchar(255) primary key,
                      pid varchar(255) not null,
                      ref varchar(255) not null,
                      cancel_flag boolean not null,
                      q bigint not null,
                      source_topic varchar(255) not null,
                      source_partition int not null,
                      source_offset bigint not null,
                      source_timestamp bigint not null
                    )
                    """);
            connection.createStatement().executeUpdate("""
                    create table if not exists accepted_s (
                      id varchar(255) primary key,
                      pid varchar(255) not null,
                      q bigint not null,
                      source_topic varchar(255) not null,
                      source_partition int not null,
                      source_offset bigint not null,
                      source_timestamp bigint not null
                    )
                    """);
            connection.createStatement().executeUpdate("""
                    create table if not exists generated_ts (
                      id varchar(255) primary key,
                      pid varchar(255) not null,
                      q bigint not null,
                      source_topic varchar(255) not null,
                      source_partition int not null,
                      source_offset bigint not null,
                      source_timestamp bigint not null
                    )
                    """);
            connection.createStatement().executeUpdate("""
                    create table if not exists unprocessed_t_state (
                      pid varchar(255) primary key,
                      t_id varchar(255) not null,
                      ref varchar(255) not null,
                      cancel_flag boolean not null,
                      q bigint not null,
                      source_topic varchar(255) not null,
                      source_partition int not null,
                      source_offset bigint not null,
                      source_timestamp bigint not null
                    )
                    """);
            connection.createStatement().executeUpdate("""
                    create table if not exists unprocessed_s_state (
                      pid varchar(255) primary key,
                      s_id varchar(255) not null,
                      q bigint not null,
                      source_topic varchar(255) not null,
                      source_partition int not null,
                      source_offset bigint not null,
                      source_timestamp bigint not null
                    )
                    """);
            connection.createStatement().executeUpdate("""
                    create table if not exists kafka_consumer_offsets (
                      consumer_group varchar(255) not null,
                      topic varchar(255) not null,
                      partition_id int not null,
                      next_offset bigint not null,
                      primary key (consumer_group, topic, partition_id)
                    )
                    """);
            connection.createStatement().executeUpdate("""
                    create table if not exists db_sync_applied_events (
                      event_id varchar(512) primary key
                    )
                    """);
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to initialize db sync schema", exception);
        }
    }


    DataSource dataSource() {
        return dataSource;
    }

    @Override
    public Map<Integer, Long> loadOffsets(String consumerGroup, String topic) {
        Map<Integer, Long> offsets = new HashMap<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select partition_id, next_offset
                     from kafka_consumer_offsets
                     where consumer_group = ? and topic = ?
                     """)) {
            statement.setString(1, consumerGroup);
            statement.setString(2, topic);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    offsets.put(resultSet.getInt(1), resultSet.getLong(2));
                }
            }
            return offsets;
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to load offsets", exception);
        }
    }

    @Override
    public void applyBatch(String consumerGroup, String topic, int partition, long nextOffset, List<DbSyncEnvelope> envelopes) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                for (DbSyncEnvelope envelope : envelopes) {
                    if (alreadyApplied(connection, envelope.eventId())) {
                        continue;
                    }
                    applyMutation(connection, envelope);
                    markApplied(connection, envelope.eventId());
                }
                upsertOffset(connection, consumerGroup, topic, partition, nextOffset);
                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to apply db-sync batch", exception);
        }
    }

    private static boolean alreadyApplied(Connection connection, String eventId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select 1 from db_sync_applied_events where event_id = ?")) {
            statement.setString(1, eventId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static void markApplied(Connection connection, String eventId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into db_sync_applied_events(event_id) values (?)")) {
            statement.setString(1, eventId);
            statement.executeUpdate();
        }
    }

    private static void applyMutation(Connection connection, DbSyncEnvelope envelope) throws SQLException {
        DbSyncMutationType mutationType = envelope.mutationType();
        switch (mutationType) {
            case ACCEPTED_T -> upsertAcceptedT(connection, envelope);
            case ACCEPTED_S -> upsertAcceptedS(connection, envelope);
            case GENERATED_TS -> upsertGeneratedTs(connection, envelope);
            case UPSERT_UNPROCESSED_T -> upsertUnprocessedT(connection, envelope);
            case UPSERT_UNPROCESSED_S -> upsertUnprocessedS(connection, envelope);
            case DELETE_UNPROCESSED_T -> deleteUnprocessedT(connection, envelope.pid());
            case DELETE_UNPROCESSED_S -> deleteUnprocessedS(connection, envelope.pid());
        }
    }

    private static void upsertAcceptedT(Connection connection, DbSyncEnvelope envelope) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                merge into accepted_t (id, pid, ref, cancel_flag, q, source_topic, source_partition, source_offset, source_timestamp)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, envelope.t().id());
            statement.setString(2, envelope.pid());
            statement.setString(3, envelope.t().ref());
            statement.setBoolean(4, envelope.t().cancel());
            statement.setLong(5, envelope.t().q());
            bindSource(statement, envelope, 6);
            statement.executeUpdate();
        }
    }

    private static void upsertAcceptedS(Connection connection, DbSyncEnvelope envelope) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                merge into accepted_s (id, pid, q, source_topic, source_partition, source_offset, source_timestamp)
                values (?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, envelope.s().id());
            statement.setString(2, envelope.pid());
            statement.setLong(3, envelope.s().q());
            bindSource(statement, envelope, 4);
            statement.executeUpdate();
        }
    }

    private static void upsertGeneratedTs(Connection connection, DbSyncEnvelope envelope) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                merge into generated_ts (id, pid, q, source_topic, source_partition, source_offset, source_timestamp)
                values (?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, envelope.ts().id());
            statement.setString(2, envelope.pid());
            statement.setLong(3, envelope.ts().q());
            bindSource(statement, envelope, 4);
            statement.executeUpdate();
        }
    }

    private static void upsertUnprocessedT(Connection connection, DbSyncEnvelope envelope) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                merge into unprocessed_t_state (pid, t_id, ref, cancel_flag, q, source_topic, source_partition, source_offset, source_timestamp)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, envelope.pid());
            statement.setString(2, envelope.t().id());
            statement.setString(3, envelope.t().ref());
            statement.setBoolean(4, envelope.t().cancel());
            statement.setLong(5, envelope.t().q());
            bindSource(statement, envelope, 6);
            statement.executeUpdate();
        }
    }

    private static void upsertUnprocessedS(Connection connection, DbSyncEnvelope envelope) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                merge into unprocessed_s_state (pid, s_id, q, source_topic, source_partition, source_offset, source_timestamp)
                values (?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, envelope.pid());
            statement.setString(2, envelope.s().id());
            statement.setLong(3, envelope.s().q());
            bindSource(statement, envelope, 4);
            statement.executeUpdate();
        }
    }

    private static void deleteUnprocessedT(Connection connection, String pid) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("delete from unprocessed_t_state where pid = ?")) {
            statement.setString(1, pid);
            statement.executeUpdate();
        }
    }

    private static void deleteUnprocessedS(Connection connection, String pid) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("delete from unprocessed_s_state where pid = ?")) {
            statement.setString(1, pid);
            statement.executeUpdate();
        }
    }

    private static void upsertOffset(Connection connection, String consumerGroup, String topic, int partition, long nextOffset) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                merge into kafka_consumer_offsets(consumer_group, topic, partition_id, next_offset)
                values (?, ?, ?, ?)
                """)) {
            statement.setString(1, consumerGroup);
            statement.setString(2, topic);
            statement.setInt(3, partition);
            statement.setLong(4, nextOffset);
            statement.executeUpdate();
        }
    }

    private static void bindSource(PreparedStatement statement, DbSyncEnvelope envelope, int startIndex) throws SQLException {
        statement.setString(startIndex, envelope.sourceTopic());
        statement.setInt(startIndex + 1, envelope.sourcePartition());
        statement.setLong(startIndex + 2, envelope.sourceOffset());
        statement.setLong(startIndex + 3, envelope.sourceTimestamp());
    }
}
