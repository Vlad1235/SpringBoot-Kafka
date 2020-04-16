package com.learnkafka.domain;


import javax.validation.Valid;
import javax.validation.constraints.NotNull;
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
  @NotNull
  @Valid
  private Book book;

}
