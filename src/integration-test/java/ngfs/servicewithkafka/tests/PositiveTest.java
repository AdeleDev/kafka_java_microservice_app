package ngfs.servicewithkafka.tests;

import ngfs.servicewithkafka.model.Address;
import ngfs.servicewithkafka.model.HousePayload;
import ngfs.servicewithkafka.model.People;
import ngfs.servicewithkafka.model.PeoplePayload;
import ngfs.servicewithkafka.repository.PeopleRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.springframework.test.util.AssertionErrors.assertEquals;
import static org.springframework.test.util.AssertionErrors.assertNotNull;

@SpringBootTest
public class PositiveTest extends TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositiveTest.class);

    private KafkaConsumer<Integer, PeoplePayload> consumerPeople;

    private KafkaConsumer<Integer, PeoplePayload> consumerPeople2;

    private KafkaConsumer<Integer, HousePayload> consumerHouses;

    private KafkaProducer<Integer, HousePayload> producerHouses;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private PeopleRepository repository;


    @BeforeEach
    public void init() {
        consumerPeople = new KafkaConsumer<>(getConsumerConfig(bootstrapServers));
        consumerPeople2 = new KafkaConsumer<>(getConsumerConfig(bootstrapServers));
        Map<String, Object> consumerConfigs = getConsumerConfig(bootstrapServers);
        consumerConfigs.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "ngfs.servicewithkafka.model.HousePayload");
        consumerHouses = new KafkaConsumer<>(consumerConfigs);
        producerHouses = new KafkaProducer<>(getProducerConfig(bootstrapServers));
    }

    @AfterEach
    public void closeConsumers() {
        consumerPeople.close();
        consumerPeople2.close();
        consumerHouses.close();
        producerHouses.close();
    }

    @Test
    public void testReceivingKafkaEventsUpdate() {
        HousePayload housePayload = getHouseProducerPayload("update");
        Address address = getAddress(123, new long[]{1, 2, 3});
        People person = getPeople(1, "Jan", "Elling", LocalDate.now(), address, List.of(1L, 2L, 3L),
                List.of(1L, 2L, 3L, 4L));

        address = getAddress(housePayload.getEventId(), new long[]{1, 2});
        People person2 = getPeople(2, "Kersten", "Siem", LocalDate.now(), address, List.of(1L),
                List.of(1L, 2L));

        repository.save(person);
        repository.save(person2);

        LOGGER.info("Send notification event..");
        consumerPeople.subscribe(Collections.singleton(PEOPLE_TOPIC));
        consumerPeople.poll(Duration.ofSeconds(10));

        sendHouseProducerRecord(housePayload, producerHouses);

        //get record sent by service
        ConsumerRecord<Integer, PeoplePayload> latestRecordPeople = getLatestRecordInTopic(consumerPeople);
        assertNotNull("Payload for topic " + PEOPLE_TOPIC + " is null, no messages found in topic", latestRecordPeople);
        assertEquals("Key for topic " + PEOPLE_TOPIC + " is wrong",
                Math.toIntExact(housePayload.getEvent().getId()), latestRecordPeople.key());

        Assertions.assertSame(1, repository.findById(person2.getId()).get().getAddressData().getHomeIds().length);
    }

    @Test
    public void testReceivingKafkaEventsDelete() {
        HousePayload housePayload = getHouseProducerPayload("delete");

        Address address = getAddress(123, new long[]{1, 2, 3});
        People person = getPeople(1, "Jan", "Elling", LocalDate.now(), address, List.of(1L, 2L, 3L),
                List.of(1L, 2L, 3L, 4L));

        address = getAddress(housePayload.getEventId(), new long[]{1, 2, 3});
        People person2 = getPeople(2, "Kersten", "Siem", LocalDate.now(), address, List.of(1L),
                List.of(1L, 2L));

        repository.save(person);
        repository.save(person2);

        LOGGER.info("Send notification event..");
        consumerPeople.subscribe(Collections.singleton(PEOPLE_TOPIC));
        consumerPeople.poll(Duration.ofSeconds(10));

        consumerPeople2.subscribe(Collections.singleton(PEOPLE_TOPIC2));
        consumerPeople2.poll(Duration.ofSeconds(10));

        sendHouseProducerRecord(housePayload, producerHouses);

        //get record sent by service
        ConsumerRecord<Integer, PeoplePayload> latestRecordPeople = getLatestRecordInTopic(consumerPeople);
        ConsumerRecord<Integer, PeoplePayload> latestRecordPeople2 = getLatestRecordInTopic(consumerPeople2);

        assertNotNull("Payload for topic " + PEOPLE_TOPIC + " is null, no messages found in topic", latestRecordPeople);
        assertEquals("Key for topic " + PEOPLE_TOPIC + " is wrong",
                Math.toIntExact(housePayload.getEvent().getId()), latestRecordPeople.key());
        assertNotNull("Payload for topic " + PEOPLE_TOPIC2 + " is null, no messages found in topic", latestRecordPeople);
        assertEquals("Key for topic " + PEOPLE_TOPIC2 + " is wrong",
                Math.toIntExact(housePayload.getEvent().getId()), latestRecordPeople2.key());


        Assertions.assertSame(2, repository.findById(person2.getId()).get().getAddressData().getHomeIds().length);
    }
}