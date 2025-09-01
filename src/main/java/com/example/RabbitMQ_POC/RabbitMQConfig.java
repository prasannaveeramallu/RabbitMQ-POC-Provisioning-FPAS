package com.example.RabbitMQ_POC;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    public static final String PROVISION_QUEUE = "provisionQueue";

    @Bean
    public Queue provisionQueue() {
        return new Queue(PROVISION_QUEUE, true);
    }
}
