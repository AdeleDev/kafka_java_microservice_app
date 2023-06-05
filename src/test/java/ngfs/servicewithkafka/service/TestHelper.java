package ngfs.servicewithkafka.service;

import ngfs.servicewithkafka.model.People;
import ngfs.servicewithkafka.model.PeoplePayload;

import java.time.LocalDate;
import java.util.List;

public class TestHelper {
    protected static final String PEOPLE_TOPIC = "blueprints.house-people.people";

    protected static final String PEOPLE_TOPIC2 = "blueprints.house-people.people2";

    protected PeoplePayload getPersonPayload(String eventType) {
        PeoplePayload payload = new PeoplePayload();
        People person = new People();
        person.setFirstname("test");
        person.setLastname("test");
        person.setBirthday(LocalDate.now());
        person.setRelatives(List.of(1L, 2L));

        payload.setEventType(eventType);
        payload.setEvent(person);
        return payload;
    }
}
