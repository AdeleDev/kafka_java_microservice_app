package ngfs.servicewithkafka.service;

import ngfs.servicewithkafka.model.People;
import ngfs.servicewithkafka.model.PeoplePayload;
import ngfs.servicewithkafka.repository.PeopleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

import javax.validation.*;
import java.util.Objects;
import java.util.Set;

@Service
public class PublisherServiceImpl implements PublisherService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublisherServiceImpl.class);

    @Value("${spring.kafka.topics.people}")
    String PEOPLE_TOPIC;

    @Value("${spring.kafka.topics.people2}")
    String PEOPLE_TOPIC2;

    private KafkaTemplate<Integer, Object> kafkaTemplate;

    private PeopleRepository repository;

    @Autowired
    public void setRepository(PeopleRepository repository) {
        this.repository = repository;
    }

    @Override
    public void notifyPeopleChanges(Integer key, PeoplePayload peoplePayload) {
        Message<PeoplePayload> message = MessageBuilder.withPayload(peoplePayload)
                .setHeader(KafkaHeaders.TOPIC, PEOPLE_TOPIC)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .build();

        LOGGER.info("Message will be send to topic " + PEOPLE_TOPIC);

        Assert.notNull(peoplePayload.getEventType(), "Payload type is null");
        if (!"create".equals(peoplePayload.getEventType())) {
            LOGGER.info("Event type was \"update\", person with id " + peoplePayload.getEvent().getId() +
                    " will be updated in repository");
            repository.save(peoplePayload.getEvent());
        }
        kafkaSendAndLog(message);

    }

    @Override
    public void notifyAnotherPeopleChanges(Integer key, PeoplePayload peoplePayload) {
        LOGGER.info("Message will be send to topic " + PEOPLE_TOPIC2);
        Message<PeoplePayload> message = MessageBuilder.withPayload(peoplePayload)
                .setHeader(KafkaHeaders.TOPIC, PEOPLE_TOPIC2)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .build();
        kafkaSendAndLog(message);
    }

    private void kafkaSendAndLog(Message<PeoplePayload> message) {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<People>> violations = validator.validate(message.getPayload().getEvent());
        if (violations.size() != 0) {
            throw new ConstraintViolationException(violations);
        }
        ListenableFuture<SendResult<Integer, Object>> future = kafkaTemplate.send(message);
        kafkaTemplate.flush();
        future.addCallback(
                result -> LOGGER.info("Message was successfully sent: " + Objects.requireNonNull(result)),
                ex -> LOGGER.error("Error has occurred: " + ex.getMessage()));
    }

    @Autowired
    public void setKafkaTemplate(KafkaTemplate<Integer, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
}

