package com.example.RabbitMQ_POC.worker;

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
import java.util.*;

public class ProvisioningUtils {
    public static Map<String, Object> provisionS3(Map<String, Object> config, String region, String accessKey, String secretKey) {
        String bucketName = (String) config.get("bucketName");
        S3Client s3 = S3Client.builder()
            .region(Region.of(region))
            .credentialsProvider(
                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)
                )
            )
            .build();
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
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

    public static Map<String, Object> provisionEC2(Map<String, Object> config, String region, String accessKey, String secretKey) {
        Ec2Client ec2 = Ec2Client.builder()
            .region(Region.of(region))
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
                    .tags(software.amazon.awssdk.services.ec2.model.Tag.builder().key("Name").value("my-ec2-instance").build())
                    .build())
                .build();
            RunInstancesResponse runResponse = ec2.runInstances(runRequest);
            String instanceId = runResponse.instances().get(0).instanceId();
            Ec2Waiter waiter = ec2.waiter();
            waiter.waitUntilInstanceRunning(DescribeInstancesRequest.builder().instanceIds(instanceId).build());
            return Map.of("status", "success", "instanceId", instanceId);
        } catch (Exception e) {
            return Map.of("status", "failed", "error", e.getMessage());
        }
    }

    public static Map<String, Object> provisionRDS(Map<String, Object> config, String region, String accessKey, String secretKey) {
        RdsClient rds = RdsClient.builder()
            .region(Region.of(region))
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
                    .dbInstanceClass((String) config.getOrDefault("DBInstanceClass", "db.t3.micro"))
                    .build();
                rds.createDBInstance(instanceRequest);
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

    public static Map<String, Object> provisionAzureVM(
        Map<String, Object> config,
        String clientId,
        String clientSecret,
        String tenantId,
        String subscriptionId,
        String secretId,
        String resourceGroup,
        String location
    ) {
        try {
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
            // For production, use LRO pollers for async polling of long-running operations
            // Example: PollerFlux<Void, VirtualMachine> poller = vmDefinition.createAsync().toPoller();
            // poller.subscribe(response -> System.out.println("Status: " + response.getStatus()));
            // VirtualMachine vm = poller.blockLast().getFinalResult();
            VirtualMachine vm = azure.virtualMachines().define((String) config.get("vmName"))
                .withRegion(com.azure.core.management.Region.fromName(location))
                .withExistingResourceGroup(resourceGroup)
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
