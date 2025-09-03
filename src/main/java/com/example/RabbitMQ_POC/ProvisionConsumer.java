package com.example.RabbitMQ_POC;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Component
public class ProvisionConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProvisionConsumer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "provisionQueue")
    public void receiveMessage(String message) {
        try {
            ProvisionRequest request = objectMapper.readValue(message, ProvisionRequest.class);
            Path dir = Paths.get("jobs");
            Files.createDirectories(dir);
            Path jobFile = dir.resolve(request.getJobId() + ".json");
            Files.writeString(jobFile, message, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Queued job {} with {} resources. Job file: {}", request.getJobId(), request.getResources() != null ? request.getResources().size() : 0, jobFile.toAbsolutePath());
            DockerWorkerLauncher.launchAsync(jobFile, request.getJobId());
        } catch (Exception e) {
            log.error("Failed to process message", e);
        }
    }
}
