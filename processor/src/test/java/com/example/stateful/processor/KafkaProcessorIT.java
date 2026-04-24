package com.example.stateful.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.messaging.MessageEnvelope;
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
        String applicationId = "stateful-it-" + suffix;

        createTopics(inputTopic, outputTopic);

        try (ConfigurableApplicationContext context = ProcessorApplication.createApplication().run(
                "--spring.kafka.bootstrap-servers=" + BOOTSTRAP,
                "--app.application-id=" + applicationId,
                "--app.input-topic=" + inputTopic,
                "--app.output-topic=" + outputTopic,
                "--app.state-dir=" + Files.createTempDirectory("stateful-it-state"),
                "--app.instance.partition-number=0",
                "--app.instance.total-partitions=3",
                "--app.commit-interval-ms=100"
        )) {
            KafkaStreamsManager manager = context.getBean(KafkaStreamsManager.class);
            assertThat(manager.waitUntilRunning(Duration.ofSeconds(30))).isTrue();

            SerdeFactory serdeFactory = context.getBean(SerdeFactory.class);

            try (KafkaProducer<String, MessageEnvelope> producer = producer(serdeFactory);
                 KafkaConsumer<String, MessageEnvelope> consumer = consumer(serdeFactory, outputTopic)) {

                producer.send(new ProducerRecord<>(inputTopic, "IBM", MessageEnvelope.forT(new T("t-101", "IBM", 1000L)))).get();
                producer.send(new ProducerRecord<>(inputTopic, "IBM", MessageEnvelope.forS(new S("s-101", "IBM", 500L)))).get();
                producer.flush();

                ConsumerRecord<String, MessageEnvelope> output = pollOne(consumer, Duration.ofSeconds(30));
                assertThat(output.key()).isEqualTo("IBM");
                assertThat(output.value().kind().name()).isEqualTo("TS");
                assertThat(output.value().ts().id()).isEqualTo("ts-t-101");
                assertThat(output.value().ts().pid()).isEqualTo("IBM");
                assertThat(output.value().ts().q()).isEqualTo(1000L);

                Optional<TBucket> tBucket = waitFor(() -> manager.readUnprocessedT("IBM"), Duration.ofSeconds(15));
                Optional<SBucket> sBucket = waitFor(() -> manager.readUnprocessedS("IBM"), Duration.ofSeconds(15));

                assertThat(tBucket).isPresent();
                assertThat(tBucket.get().items()).hasSize(1);
                assertThat(tBucket.get().items().get(0).id()).isEqualTo("t-101");

                assertThat(sBucket).isPresent();
                assertThat(sBucket.get().items()).hasSize(1);
                assertThat(sBucket.get().items().get(0).id()).isEqualTo("s-101");
            }
        }
    }

    private static void createTopics(String inputTopic, String outputTopic) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(
                    new NewTopic(inputTopic, 3, (short) 1),
                    new NewTopic(outputTopic, 3, (short) 1)
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

    private static ConsumerRecord<String, MessageEnvelope> pollOne(KafkaConsumer<String, MessageEnvelope> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, MessageEnvelope> records = consumer.poll(Duration.ofMillis(500));
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
