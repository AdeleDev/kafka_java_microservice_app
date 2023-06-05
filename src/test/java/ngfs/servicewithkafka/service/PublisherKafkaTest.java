package ngfs.servicewithkafka.service;

import ngfs.servicewithkafka.model.Address;
import ngfs.servicewithkafka.model.HousePayload;
import ngfs.servicewithkafka.model.People;
import ngfs.servicewithkafka.model.PeoplePayload;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import javax.validation.ConstraintViolationException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.util.AssertionErrors.assertEquals;

/**
 * Example of tests for kafka based on spring-kafka-test library
 */
@SpringBootTest()
@RunWith(SpringRunner.class)
@TestPropertySource(properties = {"spring.kafka.topics.people=blueprints.house-people.people",
        "spring.kafka.topics.people2=blueprints.house-people.people2",
        "spring.kafka.topics.houses=blueprints.house-people.house"})
public class PublisherKafkaTest extends TestHelper {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, false, 1, PEOPLE_TOPIC, PEOPLE_TOPIC2);

    private static final EmbeddedKafkaBroker embeddedKafkaBroker = embeddedKafka.getEmbeddedKafka();

    Consumer<Integer, PeoplePayload> consumerPeople;

    private final People person = new People();

    @Autowired
    private PublisherServiceImpl publisherService;

    @DynamicPropertySource
    public static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", embeddedKafkaBroker::getBrokersAsString);
    }

    @Before
    public void init() {
        Map<String, Object> consumerConfigs = new HashMap<>(
                KafkaTestUtils.consumerProps("consumer", "true", embeddedKafkaBroker));
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerPeople = new DefaultKafkaConsumerFactory<>(consumerConfigs, new IntegerDeserializer(),
                new JsonDeserializer<>(PeoplePayload.class)).createConsumer();
        consumerPeople.subscribe(Collections.singleton(PEOPLE_TOPIC));
        consumerPeople.poll(Duration.ZERO);

        person.setFirstname("firstname");
        person.setLastname("lastname");
        person.setBirthday(LocalDate.now());
        person.setStatus(People.StatusEnum.ALIVE);
    }

    @After
    public void close() {
        consumerPeople.close();
    }

    @Test
    public void peopleProducerTestCreate() {
        PeoplePayload payload = getPersonPayload("create");
        KafkaTestUtils.getRecords(consumerPeople);
        publisherService.notifyPeopleChanges(1, payload);
        checkProducer(PEOPLE_TOPIC);
    }

    @Test
    public void peopleProducerTestUpdate() {
        PeoplePayload payload = getPersonPayload("update");
        payload.getEvent().setAddressData(new Address());
        KafkaTestUtils.getRecords(consumerPeople);
        publisherService.notifyPeopleChanges(1, payload);
        checkProducer(PEOPLE_TOPIC);
    }

    @Test
    public void peopleProducerTestDelete() {
        consumerPeople.unsubscribe();
        consumerPeople.subscribe(Collections.singleton(PEOPLE_TOPIC2));
        consumerPeople.poll(Duration.ZERO);

        PeoplePayload payload = getPersonPayload("delete");
        payload.getEvent().setAddressData(new Address());
        KafkaTestUtils.getRecords(consumerPeople);
        publisherService.notifyAnotherPeopleChanges(1, payload);
        checkProducer(PEOPLE_TOPIC2);
    }

    private void checkProducer(String topic) {
        ConsumerRecord<Integer, PeoplePayload> singleRecord = KafkaTestUtils.getSingleRecord(consumerPeople, topic);
        assertEquals("Key is wrong", 1, singleRecord.key());
        assertNotEquals("Key is wrong", 2, singleRecord.key().intValue());
    }

    @Test
    public void testIdConstraints() {
        person.setId(1000000001L);
        peopleExceptionAssert(person, "id: must be less than or equal to 1000000000");
    }

    @Test
    public void tesBirthdayConstraints() {
        person.setBirthday(null);
        peopleExceptionAssert(person, "birthday: must not be null");
    }

    @Test
    public void tesFirstnameConstraints() {
        person.setFirstname(null);
        peopleExceptionAssert(person, "firstname: must not be null");

        person.setFirstname("");
        peopleExceptionAssert(person, "firstname: size must be between 1 and 20");

        person.setFirstname("TwentyOneLetters1234.");
        peopleExceptionAssert(person, "firstname: size must be between 1 and 20");
    }

    @Test
    public void tesLastnameConstraints() {
        person.setLastname(null);
        peopleExceptionAssert(person, "lastname: must not be null");

        person.setLastname("");
        peopleExceptionAssert(person, "lastname: size must be between 1 and 20");

        person.setLastname("TwentyOneLetters1234.");
        peopleExceptionAssert(person, "lastname: size must be between 1 and 20");
    }

    @Test
    public void tesRelativesConstraints() {
        person.setRelatives(List.of(1L));
        peopleExceptionAssert(person, "relatives: size must be between 2 and 2147483647");
    }

    @Test
    public void tesChildrenConstraints() {
        person.setChildren(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L));
        peopleExceptionAssert(person, "children: size must be between 0 and 10");
    }

    @Test
    public void shouldLogInCaseOfEvent() {
        int key = OffsetDateTime.now().getNano();
        MessageHandlerServiceImpl myClass = new MessageHandlerServiceImpl();

        MessageHandlerService instance = (MessageHandlerService) Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(), new Class<?>[]{MessageHandlerService.class},
                new InvocationHandler() {
                    final MessageHandlerServiceImpl target = myClass;

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if (method.getName().equals("notifyHouseChanges")) {
                            return method.invoke(target, args);
                        }
                        return method.invoke(target);
                    }
                });

        HousePayload payload = new HousePayload();
        Assertions.assertEquals(("Next message has received: key: " + key + ", Payload: " + payload + ", Timestamp: 1, Partition: 1"), instance.notifyHouseChanges(payload, key, 1, 1));
    }

    private void peopleExceptionAssert(People person, String message) {
        int key = OffsetDateTime.now().getNano();
        PeoplePayload payload = getPeoplePayload(key, person, LocalDateTime.now());
        ConstraintViolationException exception = assertThrows(ConstraintViolationException.class,
                () -> publisherService.notifyPeopleChanges(key, payload));
        String actualMessage = exception.getMessage();
        Assertions.assertTrue(actualMessage.contains(message), "Original message: " + actualMessage);
    }

    protected PeoplePayload getPeoplePayload(int id, People event, LocalDateTime sentAt) {
        PeoplePayload payload = new PeoplePayload();
        payload.setEventId(id);
        payload.setEvent(event);
        payload.setEventType("create");
        payload.setSentAt(OffsetDateTime.from(sentAt));
        return payload;
    }
}
