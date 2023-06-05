package ngfs.servicewithkafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ServiceWithKafkaApplication {
    private final static Logger LOGGER = LoggerFactory.getLogger(ServiceWithKafkaApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ServiceWithKafkaApplication.class, args);
        LOGGER.info("Application is running");
    }

}
