package ngfs.servicewithkafka.service;

import net.logstash.logback.encoder.org.apache.commons.lang3.ArrayUtils;
import ngfs.servicewithkafka.model.HousePayload;
import ngfs.servicewithkafka.model.People;
import ngfs.servicewithkafka.model.PeoplePayload;
import ngfs.servicewithkafka.repository.PeopleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import java.time.OffsetDateTime;
import java.util.List;

@Service
public class MessageHandlerServiceImpl implements MessageHandlerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerServiceImpl.class);

    private PeopleRepository repository;

    private PublisherServiceImpl publisherService;

    @Autowired
    public void setRepository(PeopleRepository repository) {
        this.repository = repository;
    }

    @Autowired
    public void setPublisherService(PublisherServiceImpl publisherService) {
        this.publisherService = publisherService;
    }

    @Override
    @KafkaListener(topics = "#{'${spring.kafka.topics.houses}'}", groupId = "${spring.kafka.houses-id}")
    public String notifyHouseChanges(@Payload @Valid HousePayload payload,
                                     @Header(KafkaHeaders.RECEIVED_KEY) Integer key,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                     @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
        String logging =
                "Next message was received: key: " + key + ", Payload: " + payload + ", Timestamp: " + timestamp +
                        ", Partition: " + partition;
        LOGGER.info(logging);
        if ("update".equals(payload.getEventType())) {
            LOGGER.info("Houses payload was \"update\", information to people topic will be published");
            List<People> people = repository.findByAddressId(payload.getEvent().getId());
            LOGGER.info("Found people with changed house: " + people);

            for (People personUpdated : people) {
                personUpdated.getAddressData().setHomeIds(new long[]{payload.getEvent().getNumber()});

                PeoplePayload peoplePayload = getPeoplePayload(key, personUpdated, payload.getEventType(),
                        OffsetDateTime.now());
                publisherService.notifyPeopleChanges(key, peoplePayload);
            }
        }
        if ("delete".equals(payload.getEventType())) {
            LOGGER.info("Houses payload was \"delete\", information to people topic will be published");
            List<People> people = repository.findByAddressId(payload.getEvent().getId());
            LOGGER.info("Found people with changed house: " + people);

            for (People personUpdated : people) {

                long[] array = ArrayUtils.removeElement(personUpdated.getAddressData().getHomeIds(),
                        payload.getEvent().getNumber());
                personUpdated.getAddressData().setHomeIds(array);

                PeoplePayload peoplePayload = getPeoplePayload(key, personUpdated, "update", OffsetDateTime.now());
                LOGGER.info("People payload to send: " + peoplePayload);
                publisherService.notifyPeopleChanges(key, peoplePayload);
                publisherService.notifyAnotherPeopleChanges(key, peoplePayload);
            }
        }
        return logging;
    }

    private PeoplePayload getPeoplePayload(int id, People event, String eventType, OffsetDateTime sentAt) {
        PeoplePayload payload = new PeoplePayload();
        payload.setEventId(id);
        payload.setEvent(event);
        payload.setEventType(eventType);
        payload.setSentAt(sentAt);
        return payload;
    }
}
