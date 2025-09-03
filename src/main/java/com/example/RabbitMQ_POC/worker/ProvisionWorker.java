package com.example.RabbitMQ_POC.worker;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class ProvisionWorker {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java ProvisionWorker <jobFile.json>");
            System.exit(1);
        }
    long start = System.currentTimeMillis();
    String version = System.getenv().getOrDefault("WORKER_VERSION", "unknown");
    String commit = System.getenv().getOrDefault("WORKER_GIT_COMMIT", "unknown");
    System.out.println("[WORKER] version=" + version + " commit=" + commit + " starting provisioning");
        // Load job details from JSON file
        ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    Map<String, Object> job = mapper.readValue(new File(args[0]), Map.class);
        String jobId = (String) job.get("jobId");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resources = (List<Map<String, Object>>) job.get("resources");
        // Setup MongoDB connection
    String mongoUri = System.getenv("MONGODB_URI");
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoUri));
        Map<String, Object> jobStatus = new HashMap<>();
        jobStatus.put("jobId", jobId);
        jobStatus.put("status", "in-progress");
        jobStatus.put("resources", new ArrayList<>());
        mongoTemplate.save(jobStatus, "rabbitMQPOC");
        // Provision resources in parallel within this job container
        // Example: Access AWS and Azure credentials from environment variables
        String awsRegion = System.getenv("AWS_REGION");
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String azureClientId = System.getenv("AZURE_CLIENT_ID");
        String azureClientSecret = System.getenv("AZURE_CLIENT_SECRET");
        String azureTenantId = System.getenv("AZURE_TENANT_ID");
        String azureSubscriptionId = System.getenv("AZURE_SUBSCRIPTION_ID");
        String azureSecretId = System.getenv("AZURE_SECRET_ID");
        String azureResourceGroup = System.getenv("AZURE_RESOURCE_GROUP");
        String azureLocation = System.getenv("AZURE_LOCATION");

        int poolSize = Math.min(resources.size(), 5); // cap threads
        ExecutorService exec = Executors.newFixedThreadPool(poolSize);
        List<Future<Map<String, Object>>> futures = new ArrayList<>();
        for (Map<String, Object> resource : resources) {
            futures.add(exec.submit((Callable<Map<String, Object>>) () -> {
                String type = ((String) resource.get("type")).toLowerCase();
                @SuppressWarnings("unchecked")
                Map<String, Object> config = (Map<String, Object>) resource.get("config");
                // Pass environment variables to ProvisioningUtils as needed
                try {
                    return switch (type) {
                        case "s3" -> ProvisioningUtils.provisionS3(config, awsRegion, awsAccessKey, awsSecretKey); // Update method signature in ProvisioningUtils
                        case "ec2" -> ProvisioningUtils.provisionEC2(config, awsRegion, awsAccessKey, awsSecretKey); // Update method signature in ProvisioningUtils
                        case "rds" -> ProvisioningUtils.provisionRDS(config, awsRegion, awsAccessKey, awsSecretKey); // Update method signature in ProvisioningUtils
                        case "azure-vm" -> ProvisioningUtils.provisionAzureVM(config, azureClientId, azureClientSecret, azureTenantId, azureSubscriptionId, azureSecretId, azureResourceGroup, azureLocation); // Update method signature in ProvisioningUtils
                        default -> Map.of("status", "failed", "error", (Object) "Unknown resource type");
                    };
                } catch (Exception e) {
                    return Map.of("status", "failed", "error", (Object) e.getMessage());
                }
            }));
        }
        exec.shutdown();
        List<Map<String, Object>> results = new ArrayList<>();
        int successCount = 0;
        int failCount = 0;
        for (Future<Map<String, Object>> f : futures) {
            Map<String, Object> res;
            try { res = f.get(); } catch (Exception e) { res = Map.of("status", "failed", "error", e.getMessage()); }
            results.add(res);
            if ("success".equals(res.get("status"))) successCount++; else failCount++; }
        String finalStatus = (successCount == resources.size()) ? "completed" :
                (successCount > 0 ? "partially-failed" : "failed");
        jobStatus.put("status", finalStatus);
        jobStatus.put("resources", results);
        mongoTemplate.save(jobStatus, "rabbitMQPOC");
    long durationMs = System.currentTimeMillis() - start;
        System.out.println("[WORKER] job=" + jobId + " status=" + finalStatus + " success=" + successCount + " failed=" + failCount + " durationMs=" + durationMs);

        // Send notification to RabbitMQ
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(System.getenv().getOrDefault("RABBITMQ_HOST", "host.docker.internal"));
            factory.setPort(5672);
            factory.setUsername(System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest"));
            factory.setPassword(System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest"));
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare("notificationQueue", false, false, false, null);
            String message = "Job " + jobId + " " + finalStatus + " (success: " + successCount + ", failed: " + failCount + ")";
            channel.basicPublish("", "notificationQueue", null, message.getBytes());
            System.out.println("[WORKER] Notification sent: " + message);
            channel.close();
            connection.close();
        } catch (Exception e) {
            System.err.println("[WORKER] Failed to send notification: " + e.getMessage());
        }

    // Always exit 0 (business outcome recorded in Mongo). Non-zero exit will only occur on uncaught runtime errors.
    System.exit(0);
    }
}
