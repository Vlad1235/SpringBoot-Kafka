package com.learnkafka.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Указываем какой класс тестировать(Controller Layer).
 * Unit Test ы будут сканировать и проверять лишь класс, который указан.
 * Все доп зависимости будут через моки прописаны. Это отличие от интерграциионных тестов.Там вся иерархия классов
 */
@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

  @Autowired
  MockMvc mockMvc; // доступ по всем контроллерам тестируемого класса

  @MockBean
  LibraryEventProducer libraryEventProducer;

  ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void postLibraryEvent() throws Exception {
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

    String json = objectMapper.writeValueAsString(libraryEvent);

    doNothing().when(libraryEventProducer).sendLibraryEvent_approach3(isA(LibraryEvent.class)); // обучаем

    // when
    mockMvc.perform(post("/v1/libraryevent")
            .content(json)
            .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated());



    // then


  }
}
