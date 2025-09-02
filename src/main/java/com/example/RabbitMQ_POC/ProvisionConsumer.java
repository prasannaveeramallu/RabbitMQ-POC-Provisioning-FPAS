package com.example.RabbitMQ_POC;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class ProvisionConsumer {
    @Autowired
    private ProvisioningService provisioningService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    //manage threads - for parallel request processing
    private final ExecutorService executor = Executors.newFixedThreadPool(5); // max 5 parallel jobs

    @RabbitListener(queues = "provisionQueue") //listens to the queue for any new msgs
    public void receiveMessage(String message) {
        
        //send the resquest/msg to the thread
        executor.submit(() -> {
            try {
                ProvisionRequest request = objectMapper.readValue(message, ProvisionRequest.class);
                provisioningService.handleProvisionRequest(request);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
