package ngfs.servicewithkafka.repository;

import ngfs.servicewithkafka.model.People;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class PeopleRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeopleRepository.class);

    private final Map<Long, People> peopleMap = new HashMap<>();

    public void save(People people) {
        peopleMap.put(people.getId(), people);
        LOGGER.info("Person was saved with id " + people.getId() + " and address id: " +
                people.getAddressData().getRegistrationId());
    }

    public List<People> findByAddressId(Long id) {
        LOGGER.info("Search by address id..");
        List<People> list = new ArrayList<>();
        for (long i = 1; i <= peopleMap.size(); i++) {
            if (peopleMap.get(i).getAddressData().getRegistrationId() == id) {
                list.add(peopleMap.get(i));
            }
        }
        return list;
    }

    public Optional<People> findById(Long id) {
        return Optional.ofNullable(peopleMap.get(id));
    }
}
