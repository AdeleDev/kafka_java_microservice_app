package ngfs.servicewithkafka.service;

import ngfs.servicewithkafka.model.HousePayload;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import javax.validation.Valid;

public interface MessageHandlerService {

    String notifyHouseChanges(@Payload @Valid HousePayload payload,
                              @Header(KafkaHeaders.RECEIVED_KEY) Integer key,
                              @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                              @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp);

}
