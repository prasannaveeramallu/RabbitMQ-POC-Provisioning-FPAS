package com.example.RabbitMQ_POC;


import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import java.util.*;
import java.util.concurrent.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;
import software.amazon.awssdk.regions.Region;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import com.azure.resourcemanager.compute.models.VirtualMachine;
import com.azure.resourcemanager.compute.models.KnownLinuxVirtualMachineImage;
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes;

import com.azure.core.management.profile.AzureProfile;
import com.azure.core.management.AzureEnvironment;

@Service
public class ProvisioningService {
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private SimpMessagingTemplate simpMessagingTemplate;

    private static final String COLLECTION = "rabbitMQPOC";
    //this executor is for provisioning resources in parallel
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    public void handleProvisionRequest(ProvisionRequest request) {
        String jobId = request.getJobId();
        Map<String, Object> jobStatus = new HashMap<>();
        jobStatus.put("jobId", jobId);
        jobStatus.put("status", "in-progress");
        jobStatus.put("resources", new ArrayList<>());
        mongoTemplate.save(jobStatus, COLLECTION);
        simpMessagingTemplate.convertAndSend("/topic/job-status", jobStatus);

        List<Future<Map<String, Object>>> futures = new ArrayList<>();

       // we can provision resources in a single request parallely, so some resources like s3 can be provisioned 
       //quickly without having to wait for other resources like RDS
        for (ResourceRequest resource : request.getResources()) {
            futures.add(executor.submit(() -> provisionResource(resource, jobId)));
        }

        List<Map<String, Object>> results = new ArrayList<>();
        int successCount = 0;
        int failCount = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                Map<String, Object> result = future.get();
                results.add(result);
                if ("success".equals(result.get("status"))) successCount++;
                else failCount++;
            } catch (Exception e) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("status", "failed");
                errorResult.put("error", e.getMessage());
                results.add(errorResult);
                failCount++;
            }
        }

        //figure out if all resources have been successfully provisioned and assign the status
        String finalStatus = (successCount == request.getResources().size()) ? "completed" :
                (successCount > 0 ? "partially-failed" : "failed");
        jobStatus.put("status", finalStatus);
        jobStatus.put("resources", results);
        mongoTemplate.save(jobStatus, COLLECTION);
        simpMessagingTemplate.convertAndSend("/topic/job-status", jobStatus);
    }

    private Map<String, Object> provisionResource(ResourceRequest resource, String jobId) {
        Map<String, Object> config = resource.getConfig();
        try {
            switch (resource.getType().toLowerCase()) {
                case "s3":
                    return provisionS3(config);
                case "ec2":
                    return provisionEC2(config);
                case "rds":
                    return provisionRDS(config);
                case "azure-vm":
                    return provisionAzureVM(config);
                default:
                    return Map.of("status", "failed", "error", "Unknown resource type");
            }
        } catch (Exception e) {
            return Map.of("status", "failed", "error", e.getMessage());
        }
    }

    //S3
    private Map<String, Object> provisionS3(Map<String, Object> config) {
        Dotenv dotenv = Dotenv.load();
        String accessKey = dotenv.get("AWS_ACCESS_KEY_ID");
        String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY");
        String bucketName = (String) config.get("bucketName");
        S3Client s3 = S3Client.builder()
            .region(Region.AP_SOUTH_1)
            .credentialsProvider(
                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)
                )
            )
            .build();
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            //s3 waiter
            S3Waiter waiter = s3.waiter();
            waiter.waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build());
            if (Boolean.TRUE.equals(config.get("enableVersioning"))) {
                s3.putBucketVersioning(PutBucketVersioningRequest.builder()
                    .bucket(bucketName)
                    .versioningConfiguration(VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED).build())
                    .build());
            }
            return Map.of("status", "success", "bucketName", bucketName);
        } catch (Exception e) {
            return Map.of("status", "failed", "error", e.getMessage());
        }
    }


    //EC2
    private Map<String, Object> provisionEC2(Map<String, Object> config) {
        Dotenv dotenv = Dotenv.load();
        String accessKey = dotenv.get("AWS_ACCESS_KEY_ID");
        String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY");
        Ec2Client ec2 = Ec2Client.builder()
            .region(Region.AP_SOUTH_1)
            .credentialsProvider(
                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)
                )
            )
            .build();
        try {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId((String) config.get("amiId"))
                .instanceType(InstanceType.fromValue((String) config.get("instanceType")))
                .keyName((String) config.get("keyName"))
                .minCount(1).maxCount(1)
                .tagSpecifications(TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    //using pre-configured tags
                    .tags(software.amazon.awssdk.services.ec2.model.Tag.builder().key("Name").value("my-ec2-instance").build())
                    .build())
                .build();
            RunInstancesResponse runResponse = ec2.runInstances(runRequest);
            String instanceId = runResponse.instances().get(0).instanceId();
            //ec2 waiter
            Ec2Waiter waiter = ec2.waiter();
            waiter.waitUntilInstanceRunning(DescribeInstancesRequest.builder().instanceIds(instanceId).build());
            return Map.of("status", "success", "instanceId", instanceId);
        } catch (Exception e) {
            return Map.of("status", "failed", "error", e.getMessage());
        }
    }


    //RDS
    private Map<String, Object> provisionRDS(Map<String, Object> config) {
        Dotenv dotenv = Dotenv.load();
        String accessKey = dotenv.get("AWS_ACCESS_KEY_ID");
        String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY");
        RdsClient rds = RdsClient.builder()
            .region(Region.AP_SOUTH_1)
            .credentialsProvider(
                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)
                )
            )
            .build();
        try {
            if ("mysql".equalsIgnoreCase((String) config.get("engine"))) {
                CreateDbInstanceRequest instanceRequest = CreateDbInstanceRequest.builder()
                    .dbInstanceIdentifier((String) config.get("dbInstanceIdentifier"))
                    .engine((String) config.get("engine"))
                    .masterUsername((String) config.get("masterUsername"))
                    .masterUserPassword((String) config.get("masterUserPassword"))
                    .allocatedStorage((Integer) config.get("AllocatedStorage"))
                    //using pre-configured DB instance class
                    .dbInstanceClass((String) config.getOrDefault("DBInstanceClass", "db.t3.micro"))
                    .build();
                rds.createDBInstance(instanceRequest);
                //rds waiter - waits till rds is successfully provisioned to update db
                RdsWaiter waiter = rds.waiter();
                waiter.waitUntilDBInstanceAvailable(DescribeDbInstancesRequest.builder()
                    .dbInstanceIdentifier((String) config.get("dbInstanceIdentifier")).build());
                return Map.of("status", "success", "dbInstanceIdentifier", config.get("dbInstanceIdentifier"));
            } else {
                CreateDbClusterRequest clusterRequest = CreateDbClusterRequest.builder()
                    .dbClusterIdentifier((String) config.get("dbClusterIdentifier"))
                    .engine((String) config.get("engine"))
                    .masterUsername((String) config.get("masterUsername"))
                    .masterUserPassword((String) config.get("masterUserPassword"))
                    .build();
                rds.createDBCluster(clusterRequest);
                RdsWaiter waiter = rds.waiter();
                waiter.waitUntilDBClusterAvailable(DescribeDbClustersRequest.builder()
                    .dbClusterIdentifier((String) config.get("dbClusterIdentifier")).build());
                return Map.of("status", "success", "dbClusterIdentifier", config.get("dbClusterIdentifier"));
            }
        } catch (Exception e) {
            return Map.of("status", "failed", "error", e.getMessage());
        }
    }

    //VM
    private Map<String, Object> provisionAzureVM(Map<String, Object> config) {
        try {
            Dotenv dotenv = Dotenv.load();
            String clientId = dotenv.get("AZURE_CLIENT_ID");
            String clientSecret = dotenv.get("AZURE_CLIENT_SECRET");
            String tenantId = dotenv.get("AZURE_TENANT_ID");
            String subscriptionId = dotenv.get("AZURE_SUBSCRIPTION_ID");
            ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();
            AzureProfile profile = new AzureProfile(
                tenantId,
                subscriptionId,
                AzureEnvironment.AZURE
            );
            AzureResourceManager azure = AzureResourceManager
                .authenticate(credential, profile)
                .withSubscription(subscriptionId);
            
            //Azure doesn't need waiters as the create() method in azure has the same functionality
            //as waiters in aws sdk. It waits until the VM/other services are successfully created.
            VirtualMachine vm = azure.virtualMachines().define((String) config.get("vmName"))
                .withRegion(com.azure.core.management.Region.fromName((String) config.get("region")))
                .withExistingResourceGroup((String) config.get("resourceGroup"))
                .withNewPrimaryNetwork("10.0.0.0/28")
                .withPrimaryPrivateIPAddressDynamic()
                .withNewPrimaryPublicIPAddress((String) config.get("vmName") + "-ip")
                .withPopularLinuxImage(KnownLinuxVirtualMachineImage.UBUNTU_SERVER_20_04_LTS)
                .withRootUsername((String) config.get("adminUsername"))
                .withRootPassword((String) config.get("adminPassword"))
                .withSize(VirtualMachineSizeTypes.STANDARD_B1S)
                .create();
            return Map.of("status", "success", "vmId", vm.id());
        } catch (Exception e) {
            return Map.of("status", "failed", "error", e.getMessage());
        }
    }
}
