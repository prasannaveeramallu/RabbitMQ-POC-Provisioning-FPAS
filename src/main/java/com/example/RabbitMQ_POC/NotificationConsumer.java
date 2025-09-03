package com.example.RabbitMQ_POC;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class NotificationConsumer {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @RabbitListener(queues = "notificationQueue")
    public void receiveNotification(String message) {
        log.info("Received notification: {}", message);
        // Send to WebSocket clients
        messagingTemplate.convertAndSend("/topic/notifications", message);
    }
}
