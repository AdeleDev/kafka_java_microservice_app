# Java Kafka Microservice

Simple example of Kafka microservice written on SpringBoot.
Listening to house queue and pushing notifications to people nd people2 queues

### Built With

* [![Java][Java.io]][Java-url]
* [![SpringBoot][SpringBoot.io]][SpringBoot-url]
* [![Kafka][Kafka.io]][Kafka-url]
* [![AsyncApi][AsyncApi.io]][AsyncApi-url]
* [![Junit5][Junit5.io]][Junit5-url]

## Pre-installations

#### Clone the repo:

```sh
git clone https://github.com/AdeleDev/kafka_java_microservice_app.git
```

#### Build project:

```sh
gradle clean
```

```sh
gradle build
```

#### Setup bootstrap endpoint for kafka service:

```
resources/application.yaml
port: 29092
```

## Usage

#### Build Async Generator:

```sh
gradle installAsyncApiGenerator
```

#### Build Objects from Api without copying to project:

```sh
gradle generateByAsyncApiSpec
```

#### Build Objects from Api with copying model to project:

```sh
gradle generateOnlyModelByAsyncApiSpec
gradle generateAllArtifactsByAsyncApiSpec
```

#### Start docker with kafka:
```sh
cd docker
docker-compose up -d 
```

#### Create topics in Kafka :
```
3 topics from resources/application.yaml
to connect to kafka and create/check topics  can use Offset Tool: https://kafkatool.com/download.html
```

#### Run tests:

```sh
gradle integrationTests
```

<!-- MARKDOWN LINKS & IMAGES -->

[Java.io]: https://img.shields.io/badge/-☕%20Java-blue?style=for-the-badge

[Java-url]: https://www.java.com/ru/

[SpringBoot.io]: https://img.shields.io/badge/-Springboot-green?style=for-the-badge&logo=springboot

[SpringBoot-url]: https://spring.io/projects/spring-boot

[Kafka.io]: https://img.shields.io/badge/-Apache%20Kafka-blueviolet?style=for-the-badge&logo=apachekafka

[Kafka-url]: https://kafka.apache.org/

[AsyncApi.io]: https://img.shields.io/badge/-🐍%20AsyncApi-blue?style=for-the-badge

[AsyncApi-url]: https://www.asyncapi.com/

[Junit5.io]: https://img.shields.io/badge/-JUnit5-yellow?style=for-the-badge&logo=JUnit5

[Junit5-url]: https://junit.org/junit5/