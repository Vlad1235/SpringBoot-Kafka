package com.learnkafka.controller;

import static com.learnkafka.domain.LibraryEventType.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import java.util.concurrent.ExecutionException;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

  @Autowired
  LibraryEventProducer libraryEventProducer;


  @PostMapping("/v1/libraryevent")
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
      throws JsonProcessingException, ExecutionException, InterruptedException {
    // возвращаем kafka producer. Несколько способов
    log.info("Before sendLibraryEvent");
    libraryEvent.setLibraryEventType(NEW);
//    libraryEventProducer.sendLibraryEvent_approach1(libraryEvent);
//    SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous_approach2(libraryEvent);
//    log.info("SendResult is {}",sendResult.toString());
    libraryEventProducer.sendLibraryEvent_approach3(libraryEvent);
    log.info("After sendLibraryEvent");
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }


  // TODO:PUT

}
