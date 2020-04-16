package com.learnkafka.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
@Slf4j
public class LibraryEventProducer {

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  private String topic = "library-events";

  /*
  Не синхронизованный call.Несколько потоков
   */
  public void sendLibraryEvent_approach1(LibraryEvent libraryEvent) throws JsonProcessingException {
    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent); // ковертируем объект в JSON
    ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate
        .sendDefault(key, value);
    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onFailure(Throwable ex) {
        handleFailure(key, value, ex);
      }

      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }
    });
  }

  private void handleFailure(Integer key, String value, Throwable ex) {
    log.error("Error sending the message and the exception is {}", ex.getMessage());
    try {
      throw ex;
    } catch (Throwable throwable) {
      log.error("Error in OnFailure: {}", throwable.getMessage());
    }
  }

  private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
    log.info("Message sent successfully for the key: {} and the value {}, partitions is {}", key,
        value, result.getRecordMetadata().partition());
  }

  /*
  делать synchronous call. Вся работа будет в одном потоке.
   */
  public SendResult<Integer, String> sendLibraryEventSynchronous_approach2(LibraryEvent libraryEvent)
      throws JsonProcessingException, ExecutionException, InterruptedException {
    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);
    SendResult<Integer, String> sendResult = null;
    try {
      sendResult = kafkaTemplate.sendDefault(key, value).get();
    } catch (ExecutionException | InterruptedException ex) {
      log.error(
          "ExecutionException / InterruptedException sending the message and the exception is {}",
          ex.getMessage());
      throw ex;
    } catch (Exception e) {
      log.error("Exception sending the message and the exception is {}", e.getMessage());
      throw e;
    }
    return sendResult;
  }

  public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {
    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);

    ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key,value,topic);
    ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate
        .send(producerRecord);
    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onFailure(Throwable ex) {
        handleFailure(key, value, ex);
      }

      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }
    });
    return listenableFuture;
  }

  private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
    List<Header> recordHeaders = List.of(new RecordHeader("event-source","scanner".getBytes())); // message metadata. Consumer получив данные может действовать по-разному прочитав метаданные
    return new ProducerRecord<>(topic,null,key,value,recordHeaders);
  }

}
