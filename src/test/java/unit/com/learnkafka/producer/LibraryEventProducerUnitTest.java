package com.learnkafka.producer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * Тестируем выпадение метода sendLibraryEvent_approach3 в ошибку onFailure в классе LibraryEventProducer
 */
@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

  @InjectMocks // данный класс тестируется
      LibraryEventProducer eventProducer;

  @Mock // меняем поведение в тестируемом методе
      KafkaTemplate<Integer, String> kafkaTemplate;

  @Spy // не меняем изначальное поведение в тестируемом методе.
      ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void sendLibraryEvent_approach3_failureTest() throws JsonProcessingException, ExecutionException, InterruptedException {
    // given
    Book book = Book.builder()
        .bookId(123) //
        .bookAuthor("Vlad")
        .bookName("How to test of kafka")
        .build();

    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    SettableListenableFuture future = new SettableListenableFuture();
    future.setException(new RuntimeException("Exception calling Kafka"));
    when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future); // обучаем

    //when
    assertThrows(Exception.class, () -> eventProducer.sendLibraryEvent_approach3(libraryEvent).get());
    // then

  }

  @Test
  void sendLibraryEvent_approach3_successTest() throws JsonProcessingException, ExecutionException, InterruptedException {
    // given
    Book book = Book.builder()
        .bookId(123) //
        .bookAuthor("Vlad")
        .bookName("How to test of kafka")
        .build();

    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    String jsonPayload = objectMapper.writeValueAsString(libraryEvent);
    SettableListenableFuture future = new SettableListenableFuture();
    ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(), jsonPayload);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1,
        1, 1234, System.currentTimeMillis(), 1, 2); // random data
    SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
    future.set(sendResult);
    when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future); // обучаем

    //when
    ListenableFuture<SendResult<Integer, String>> listenableFuture = eventProducer.sendLibraryEvent_approach3(libraryEvent);
    // then
    SendResult<Integer, String> sendResult1 = listenableFuture.get();
    assert sendResult.getRecordMetadata().partition()==1;
  }

}
