package edu.eci.arsw.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersDlt() {
        return TopicBuilder.name("orders.DLT")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic paymentsDlt() {
        return TopicBuilder.name("payments.DLT")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic inventoryDlt() {
        return TopicBuilder.name("inventory.DLT")
            .partitions(3)
            .replicas(1)
            .build();
    }
}