package ngfs.servicewithkafka.service;

import ngfs.servicewithkafka.model.Address;
import ngfs.servicewithkafka.model.PeoplePayload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.time.Duration;
import java.util.*;

import static java.util.Collections.emptyList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Example of tests for kafka based on testcontainers library
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = {"spring.kafka.topics.people=blueprints.house-people.people",
        "spring.kafka.topics.people2=blueprints.house-people.people2",
        "spring.kafka.topics.houses=blueprints.house-people.house"})
public class TestContainerKafkaTest extends TestHelper {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer();

    @Autowired
    private PublisherServiceImpl publisherService;

    @DynamicPropertySource
    public static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    protected static <K, V> List<ConsumerRecord<K, V>> pollForRecords(KafkaConsumer<K, V> consumer) {
        ConsumerRecords<K, V> received = consumer.poll(Duration.ofSeconds(10L));
        return received == null ? emptyList() : Lists.newArrayList(received);
    }

    @Test
    public void peopleProducerTestContainersCreate() {
        PeoplePayload payload = getPersonPayload("create");
        consumeMessages(PEOPLE_TOPIC);
        publisherService.notifyPeopleChanges(1, payload);
        checkProducer(PEOPLE_TOPIC);
    }

    @Test
    public void peopleProducerTestContainersUpdate() {
        PeoplePayload payload = getPersonPayload("update");
        payload.getEvent().setAddressData(new Address());
        consumeMessages(PEOPLE_TOPIC);
        publisherService.notifyPeopleChanges(1, payload);
        checkProducer(PEOPLE_TOPIC);
    }

    @Test
    public void peopleProducerTestContainersDelete() {
        PeoplePayload payload = getPersonPayload("delete");
        payload.getEvent().setAddressData(new Address());
        consumeMessages(PEOPLE_TOPIC2);
        publisherService.notifyAnotherPeopleChanges(1, payload);
        checkProducer(PEOPLE_TOPIC2);
    }

    private void checkProducer(String topic) {
        ConsumerRecord<Integer, Object> consumedMessage = consumeMessage(topic);
        assertEquals("Key is wrong", 1, consumedMessage.key().intValue());
        assertNotEquals("Key is wrong", 2, consumedMessage.key().intValue());
    }

    protected ConsumerRecord<Integer, Object> consumeMessage(String topic) {
        return consumeMessages(topic).stream()
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No message received"));
    }

    protected List<ConsumerRecord<Integer, Object>> consumeMessages(String topic) {
        try (KafkaConsumer<Integer, Object> consumer = createConsumer(topic)) {
            return pollForRecords(consumer);
        }
    }

    protected KafkaConsumer<Integer, Object> createConsumer(String topic) {
        Properties properties = new Properties();
        properties.putAll(getKafkaConsumerConfiguration());
        KafkaConsumer<Integer, Object> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    protected Map<String, Object> getKafkaConsumerConfiguration() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configs.put(GROUP_ID_CONFIG, "testGroup");
        configs.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        return configs;
    }
}
