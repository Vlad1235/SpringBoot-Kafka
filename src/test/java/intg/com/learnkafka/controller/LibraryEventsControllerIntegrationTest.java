package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT) // every time random port at the start of app. To avoid conflict with 8080 port
/*
Аннотации ниже для использования встроенного Kafka в SpringBootTest. Не нужно подключение к реальному Kafka и Zookeeper
SpringBootTest Kafka будет работать только во время теста.
Даем базовые конфиги
Переопределяем брокера а/ов на портах localhost на встренного.
Переопределяем admin
 */
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {
    "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {

  @Autowired
  TestRestTemplate restTemplate;

  @Autowired
  EmbeddedKafkaBroker embeddedKafkaBroker;

  private Consumer<Integer, String> consumer;

  @BeforeEach
  void setUp() {
    /*
    Создаем Consumer наших сообщений. Доходят ли до него
    */
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("anyGroupName", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }

  @Test
  @Timeout(5) // решение проблемы getSingleRecord(consumer, "library-events"), что разные потоки могут создаваться для обработки.
  void postLibraryEvent() {
    // given
    Book book = Book.builder()
        .bookId(123)
        .bookAuthor("Vlad")
        .bookName("How to test of kafka")
        .build();

    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    HttpHeaders headers = new HttpHeaders();
    headers.set("content-type", MediaType.APPLICATION_JSON.toString());
    HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

    // when
    ResponseEntity<LibraryEvent> responseEntity = restTemplate
        .exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class); // делаем клиента, ктр запросит на наш (POST) API

    ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
    Integer recievedKey = consumerRecord.key();
    String recievedValue = consumerRecord.value();
    String expectedKey = null;
    String expectedValue = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"How to test of kafka\",\"bookAuthor\":\"Vlad\"}}";
    // then
    assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode()); // проверяем что клиент сможет отправить POST запрос Producer у
    assertEquals(expectedKey,recievedKey);
    assertEquals(expectedValue,recievedValue); // проверяем что Consumer получит сообщение клиента (не путать с Producer)
  }
}
