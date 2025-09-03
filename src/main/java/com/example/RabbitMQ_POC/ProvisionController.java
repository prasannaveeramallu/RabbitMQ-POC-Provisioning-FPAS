package com.example.RabbitMQ_POC;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/provision")
public class ProvisionController {
    @Autowired
    private RabbitTemplate rabbitTemplate;

    private static final int MAX_RESOURCES = 5;
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    @PostMapping
    public ResponseEntity<?> provisionResources(@RequestBody ProvisionRequest request) {

        //validating that its not empty
        if (request.getResources() == null || request.getResources().size() == 0) {
            return ResponseEntity.badRequest().body("No resources specified.");
        }

        //validating that there are only 5 resources
        if (request.getResources().size() > MAX_RESOURCES) {
            return ResponseEntity.badRequest().body("Maximum 5 resources allowed per request.");
        }

        String jobId = UUID.randomUUID().toString();
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);

        // add timestamp suffixes for unique names
        List<ResourceRequest> updatedResources = request.getResources().stream().map(resource -> {
            Map<String, Object> config = new HashMap<>(resource.getConfig());
            if ("s3".equalsIgnoreCase(resource.getType()) && config.containsKey("bucketName")) {
                config.put("bucketName", config.get("bucketName") + "-" + timestamp);
            }
            if ("azure-vm".equalsIgnoreCase(resource.getType()) && config.containsKey("vmName")) {
                config.put("vmName", config.get("vmName") + "-" + timestamp);
            }
            
            return new ResourceRequest(resource.getType(), config);
            
        }).collect(Collectors.toList());

        ProvisionRequest updatedRequest = new ProvisionRequest(updatedResources, jobId);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String message = objectMapper.writeValueAsString(updatedRequest);
            rabbitTemplate.convertAndSend("provisionQueue", message);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            return ResponseEntity.status(500).body("Failed to serialize request: " + e.getMessage());
        }

        Map<String, String> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("status", "submitted");
        return ResponseEntity.ok(response);
    }
}

class ProvisionRequest {
    private List<ResourceRequest> resources;
    private String jobId;

    public ProvisionRequest() {}
    public ProvisionRequest(List<ResourceRequest> resources, String jobId) {
        this.resources = resources;
        this.jobId = jobId;
    }
    public List<ResourceRequest> getResources() { return resources; }
    public void setResources(List<ResourceRequest> resources) { this.resources = resources; }
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
}

class ResourceRequest {
    private String type;
    private Map<String, Object> config;

    public ResourceRequest() {}
    public ResourceRequest(String type, Map<String, Object> config) {
        this.type = type;
        this.config = config;
    }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Map<String, Object> getConfig() { return config; }
    public void setConfig(Map<String, Object> config) { this.config = config; }
}
