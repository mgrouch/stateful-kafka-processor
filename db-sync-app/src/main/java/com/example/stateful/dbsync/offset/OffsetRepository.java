package com.example.stateful.dbsync.offset;

import com.example.stateful.dbsync.sql.TempTableSql;
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

public final class OffsetRepository {

    private final DataSource dataSource;

    public OffsetRepository(DataSource dataSource) {
        this.dataSource = dataSource;
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

    public void upsertOffsets(Connection connection,
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
}
