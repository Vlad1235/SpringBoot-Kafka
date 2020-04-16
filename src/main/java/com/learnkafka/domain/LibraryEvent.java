package com.learnkafka.domain;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
Это информация, которую будет производить producer
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {

  private Integer libraryEventId;
  private LibraryEventType libraryEventType; // нужна чтобы разграничивать новые сообщения и обновление старых
  private Book book;

}
