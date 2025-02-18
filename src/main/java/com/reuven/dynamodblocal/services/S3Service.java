package com.reuven.dynamodblocal.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.file.Paths;

public class S3Service {

  private static final Logger logger = LogManager.getLogger(S3Service.class);
    
    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY = "test-file.txt";
    private static final String FILE_PATH = "path/to/test-file.txt";

    public S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create("http://localhost:4566")) // LocalStack endpoint
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))  // LocalStack credentials
                .build();
    }

    public static void createBucket(S3Client s3) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build();

        s3.createBucket(createBucketRequest);
        logger.info("Bucket created successfully.");
    }

    public static void putObject(S3Client s3) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(KEY)
                .build();

        s3.putObject(putObjectRequest, RequestBody.fromFile(Paths.get(FILE_PATH)));
        logger.info("File uploaded successfully.");
    }

    public static void getObject(S3Client s3) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(KEY)
                .build();

        s3.getObject(getObjectRequest, ResponseTransformer.toFile(Paths.get("downloaded-file.txt")));
        logger.info("File downloaded successfully.");
    }

    public static void deleteObject(S3Client s3) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(KEY)
                .build();

        s3.deleteObject(deleteObjectRequest);
        logger.info("File deleted successfully.");
    }

    public static void deleteBucket(S3Client s3) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build();

        s3.deleteBucket(deleteBucketRequest);
        logger.info("Bucket deleted successfully.");
    }
}
