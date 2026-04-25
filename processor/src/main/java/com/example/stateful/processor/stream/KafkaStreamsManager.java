package com.example.stateful.processor.stream;

import com.example.stateful.processor.config.ProcessorSettings;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.state.SBucket;
import com.example.stateful.processor.state.StateStores;
import com.example.stateful.processor.state.TBucket;
import com.example.stateful.processor.logic.AllocationStrategy;
import com.example.stateful.processor.topology.TopologyFactory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class KafkaStreamsManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsManager.class);

    private final ProcessorSettings settings;
    private final Topology topology;
    private final KafkaStreams kafkaStreams;
    private volatile boolean running;

    public KafkaStreamsManager(ProcessorSettings settings, SerdeFactory serdeFactory, AllocationStrategy allocationStrategy) {
        this.settings = settings;
        this.topology = TopologyFactory.create(settings, serdeFactory, allocationStrategy);
        this.kafkaStreams = new KafkaStreams(topology, settings.toStreamsProperties());
        this.kafkaStreams.setStateListener((newState, oldState) ->
                log.info("Kafka Streams state changed from {} to {}", oldState, newState));
        this.kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught streams exception", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        log.info("Topology:{}{}", System.lineSeparator(), TopologyFactory.describe(topology));
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        kafkaStreams.start();
        running = true;
        log.info("Started processor applicationId={} bootstrapServers={} inputTopic={} outputTopic={} dbSyncTopic={}",
                settings.applicationId(), settings.bootstrapServers(), settings.inputTopic(), settings.outputTopic(), settings.dbSyncTopic());
    }

    @Override
    public void stop() {
        stop(() -> { });
    }

    @Override
    public void stop(Runnable callback) {
        try {
            kafkaStreams.close(Duration.ofSeconds(10));
        } finally {
            running = false;
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    public KafkaStreams.State state() {
        return kafkaStreams.state();
    }

    public boolean waitUntilRunning(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            KafkaStreams.State current = state();
            if (current == KafkaStreams.State.RUNNING) {
                return true;
            }
            sleep(Duration.ofMillis(100));
        }
        return false;
    }

    public Optional<TBucket> readUnprocessedT(String pid) {
        return Optional.ofNullable(queryStore(StateStores.UNPROCESSED_T_STORE, pid));
    }

    public Optional<SBucket> readUnprocessedS(String pid) {
        return Optional.ofNullable(queryStore(StateStores.UNPROCESSED_S_STORE, pid));
    }

    private <V> V queryStore(String storeName, String key) {
        try {
            ReadOnlyKeyValueStore<String, V> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
            );
            return store.get(key);
        } catch (InvalidStateStoreException exception) {
            return null;
        }
    }

    private static void sleep(Duration duration) {
        try {
            TimeUnit.MILLISECONDS.sleep(duration.toMillis());
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Kafka Streams", interruptedException);
        }
    }
}
