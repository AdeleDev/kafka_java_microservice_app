package ngfs.servicewithkafka.service;

import ngfs.servicewithkafka.model.PeoplePayload;

public interface PublisherService {

    void notifyPeopleChanges(Integer key, PeoplePayload peoplePayload);

    void notifyAnotherPeopleChanges(Integer key, PeoplePayload peoplePayload);
}
