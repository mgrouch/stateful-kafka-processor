package com.example.stateful.dbsync.repository;

import com.example.stateful.dbsync.batch.DbSyncBatch;
import com.example.stateful.dbsync.offset.OffsetRepository;
import com.example.stateful.dbsync.sql.SchemaSql;
import com.example.stateful.dbsync.sql.TempTableSql;
import com.example.stateful.dbsync.tx.TransactionExecutor;
import com.example.stateful.messaging.DbSyncEnvelope;
import org.apache.kafka.common.TopicPartition;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public final class DbSyncRepository {

    private final DataSource dataSource;
    private final OffsetRepository offsetRepository;
    private final TransactionExecutor transactionExecutor;

    public DbSyncRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        this.offsetRepository = new OffsetRepository(dataSource);
        this.transactionExecutor = new TransactionExecutor(dataSource);
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

        transactionExecutor.runInTransaction(connection -> {
            TempTableSql sql = TempTableSql.forConnection(connection);
            createTempTables(connection, sql);
            loadTempTables(connection, batch, sql);
            mergeFacts(connection, sql);
            mergeState(connection, sql);
            applyDeletes(connection, sql);
            offsetRepository.upsertOffsets(connection, consumerGroup, topic, batch.nextOffsetsByPartition(), sql);
        });
    }

    public Map<TopicPartition, Long> loadOffsets(String consumerGroup, String topic, java.util.Collection<Integer> partitions) {
        return offsetRepository.loadOffsets(consumerGroup, topic, partitions);
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
                statement.setLong(6, event.t().q_a_total());
                statement.setLong(7, event.t().q_a_delta_last());
                setSource(statement, event, 8);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertAcceptedSTemp())) {
            for (DbSyncEnvelope event : batch.acceptedS()) {
                statement.setString(1, event.s().id());
                statement.setString(2, event.pid());
                statement.setLong(3, event.s().q());
                statement.setLong(4, event.s().q_a());
                setSource(statement, event, 5);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertGeneratedTsTemp())) {
            for (DbSyncEnvelope event : batch.generatedTs()) {
                statement.setString(1, event.ts().id());
                statement.setString(2, event.pid());
                statement.setString(3, event.ts().tid());
                statement.setString(4, event.ts().sid());
                statement.setLong(5, event.ts().q());
                statement.setLong(6, event.ts().q_a_delta());
                statement.setLong(7, event.ts().q_a_total_after());
                setSource(statement, event, 8);
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
                statement.setLong(6, event.t().q_a_total());
                statement.setLong(7, event.t().q_a_delta_last());
                setSource(statement, event, 8);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertUpsertUnprocessedSTemp())) {
            for (DbSyncEnvelope event : batch.upsertUnprocessedS()) {
                statement.setString(1, event.pid());
                statement.setString(2, event.s().id());
                statement.setLong(3, event.s().q());
                statement.setLong(4, event.s().q_a());
                setSource(statement, event, 5);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertDeleteUnprocessedTTemp())) {
            for (DbSyncBatch.StateIdentity identity : batch.deleteUnprocessedT()) {
                statement.setString(1, identity.pid());
                statement.setString(2, identity.id());
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.insertDeleteUnprocessedSTemp())) {
            for (DbSyncBatch.StateIdentity identity : batch.deleteUnprocessedS()) {
                statement.setString(1, identity.pid());
                statement.setString(2, identity.id());
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

    private static void setSource(PreparedStatement statement, DbSyncEnvelope event, int startIndex) throws SQLException {
        statement.setString(startIndex, event.sourceTopic());
        statement.setInt(startIndex + 1, event.sourcePartition());
        statement.setLong(startIndex + 2, event.sourceOffset());
        statement.setLong(startIndex + 3, event.sourceTimestamp());
        statement.setString(startIndex + 4, event.eventId());
    }
}
