package com.reuven.dynamodblocal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
public class S3Test extends AwsTestContainer {

    private static S3Client s3Client;

    @BeforeEach
    void setup() {
        super.setup();
        // Get S3 endpoint and credentials from LocalStack
        URI endpoint = localstack.getEndpointOverride(getService());
        String accessKey = localstack.getAccessKey();
        String secretKey = localstack.getSecretKey();

        // Create S3Client
        s3Client = S3Client.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey))
                .build();
    }

//    @BeforeEach
//    void cleanup() {
    // Delete all S3 buckets
//        s3Client.listBuckets()
//                .buckets()
//                .forEach(b -> s3Client
//                        .deleteBucket(b.name()));
//    }

    @Test
    void createBucketTest() {
        String bucketName = "test-bucket";
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        boolean bucketExists = s3Client.listBuckets().buckets().stream()
                .anyMatch(b -> b.name().equals(bucketName));
        assertThat(bucketExists).isTrue();
    }

    @Test
    void putObjectTest() {
        String bucketName = "test-bucket-1";
        String objectKey = "test-object";
        String content = "test-content";
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectKey)
                        .build(),
                RequestBody.fromBytes(content.getBytes(StandardCharsets.UTF_8)));

        // Verify the object exists
        HeadObjectResponse headObjectResponse = s3Client.headObject(
                HeadObjectRequest.builder().bucket(bucketName).key(objectKey).build());
        assertThat(headObjectResponse).isNotNull();

        ResponseBytes<GetObjectResponse> object = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
                ResponseTransformer.toBytes());

        logger.info("Test passed: Bucket and object verified!");
    }

    @Override
    protected LocalStackContainer.Service getService() {
        return LocalStackContainer.Service.S3;
    }
}
