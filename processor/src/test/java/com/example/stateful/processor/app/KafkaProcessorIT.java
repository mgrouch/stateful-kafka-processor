package com.example.stateful.processor.app;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.state.SBucket;
import com.example.stateful.processor.state.TBucket;
import com.example.stateful.processor.stream.KafkaStreamsManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProcessorIT {

    private static final String BOOTSTRAP = System.getProperty("it.kafka.bootstrap", "localhost:9092");

    @Test
    void roundTripWithRealKafkaWorks() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String inputTopic = "input-events-" + suffix;
        String outputTopic = "processed-events-" + suffix;
        String dbSyncTopic = "db-sync-events-" + suffix;
        String applicationId = "stateful-it-" + suffix;

        createTopics(inputTopic, outputTopic, dbSyncTopic);

        try (ConfigurableApplicationContext context = ProcessorApplication.createApplication().run(
                "--spring.kafka.bootstrap-servers=" + BOOTSTRAP,
                "--app.application-id=" + applicationId,
                "--app.input-topic=" + inputTopic,
                "--app.output-topic=" + outputTopic,
                "--app.db-sync-topic=" + dbSyncTopic,
                "--app.state-dir=" + Files.createTempDirectory("stateful-it-state"),
                "--app.commit-interval-ms=100"
        )) {
            KafkaStreamsManager manager = context.getBean(KafkaStreamsManager.class);
            assertThat(manager.waitUntilRunning(Duration.ofSeconds(30))).isTrue();

            SerdeFactory serdeFactory = context.getBean(SerdeFactory.class);

            try (KafkaProducer<String, MessageEnvelope> producer = producer(serdeFactory);
                 KafkaConsumer<String, MessageEnvelope> consumer = consumer(serdeFactory, outputTopic);
                 KafkaConsumer<String, DbSyncEnvelope> dbConsumer = dbSyncConsumer(serdeFactory, dbSyncTopic)) {

                producer.send(new ProducerRecord<>(inputTopic, "IBM", MessageEnvelope.forT(new T("t-101", "IBM", "ref-101", false, 1000L, 0L)))).get();
                producer.send(new ProducerRecord<>(inputTopic, "IBM", MessageEnvelope.forS(new S("s-101", "IBM", 500L, 0L)))).get();
                producer.flush();

                ConsumerRecord<String, MessageEnvelope> output = pollOne(consumer, Duration.ofSeconds(30));
                ConsumerRecord<String, DbSyncEnvelope> dbSyncOutput = pollOne(dbConsumer, Duration.ofSeconds(30));
                assertThat(output.key()).isEqualTo("IBM");
                assertThat(output.value().kind().name()).isEqualTo("TS");
                assertThat(output.value().ts().id()).isEqualTo("ts-t-101");
                assertThat(output.value().ts().pid()).isEqualTo("IBM");
                assertThat(output.value().ts().q_a()).isEqualTo(500L);
                assertThat(dbSyncOutput.key()).isEqualTo("IBM");

                Optional<TBucket> tBucket = waitFor(() -> manager.readUnprocessedT("IBM"), Duration.ofSeconds(15));
                Optional<SBucket> sBucket = waitFor(() -> manager.readUnprocessedS("IBM"), Duration.ofSeconds(15));

                assertThat(tBucket).isPresent();
                assertThat(tBucket).isPresent();
                assertThat(tBucket.get().items()).hasSize(1);
                assertThat(tBucket.get().items().get(0).id()).isEqualTo("t-101");
                assertThat(tBucket.get().items().get(0).q_a()).isEqualTo(500L);

                assertThat(sBucket).isPresent();
                assertThat(sBucket.get().items()).isEmpty();
            }
        }
    }

    private static void createTopics(String inputTopic, String outputTopic, String dbSyncTopic) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(
                    new NewTopic(inputTopic, 3, (short) 1),
                    new NewTopic(outputTopic, 3, (short) 1),
                    new NewTopic(dbSyncTopic, 3, (short) 1)
            )).all().get();
        } catch (ExecutionException executionException) {
            if (!(executionException.getCause() instanceof TopicExistsException)) {
                throw executionException;
            }
        }
    }

    private static KafkaProducer<String, MessageEnvelope> producer(SerdeFactory serdeFactory) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(properties, new StringSerializer(), serdeFactory.envelopeSerde().serializer());
    }

    private static KafkaConsumer<String, MessageEnvelope> consumer(SerdeFactory serdeFactory, String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, MessageEnvelope> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), serdeFactory.envelopeSerde().deserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static KafkaConsumer<String, DbSyncEnvelope> dbSyncConsumer(SerdeFactory serdeFactory, String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, DbSyncEnvelope> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), serdeFactory.dbSyncEnvelopeSerde().deserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static <V> ConsumerRecord<String, V> pollOne(KafkaConsumer<String, V> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("Did not receive output record in time");
    }

    private static <T> Optional<T> waitFor(java.util.function.Supplier<Optional<T>> supplier, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<T> value = supplier.get();
            if (value.isPresent()) {
                return value;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interruptedException);
            }
        }
        return Optional.empty();
    }
}
