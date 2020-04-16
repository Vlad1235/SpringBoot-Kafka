package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Создаем topic
 */
@Configuration
@Profile("local") // выбор для какого profile данная конфигурация. Для других работать не будет.
public class AutoCreateConfig {
    @Bean
  public NewTopic libraryEvents(){
      NewTopic creatingTopic = TopicBuilder.name("library-events")
                  .partitions(3)
                  .replicas(3)
                  .build();
      return creatingTopic;
    }
}
