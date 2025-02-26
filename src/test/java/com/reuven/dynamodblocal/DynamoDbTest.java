package com.reuven.dynamodblocal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamoDbTest extends AwsTestContainer {

    private static DynamoDbClient dynamoDbClient;

    @BeforeEach
    void setup() {
        super.setup();
        // Get DynamoDB endpoint and credentials from LocalStack
        URI endpoint = localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB);
        String accessKey = localstack.getAccessKey();
        String secretKey = localstack.getSecretKey();

        // Create DynamoDbClient using LocalStack endpoint
        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey))
                .region(Region.US_EAST_1)  // Use appropriate region
                .build();
        // Delete all DynamoDB tables
        dynamoDbClient.listTables()
                .tableNames()
                .forEach(tableName -> dynamoDbClient
                        .deleteTable(DeleteTableRequest.builder()
                                .tableName(tableName)
                                .build()));
        assertThat(dynamoDbClient.listTables().tableNames()).isEmpty();
    }

    @Override
    protected LocalStackContainer.Service getService() {
        return LocalStackContainer.Service.DYNAMODB;
    }


    @Test
    void dynamoDbCreateTableTest() {
        String tableName = "TestTable";
        createTable(tableName);
        List<String> tableNames = dynamoDbClient.listTables().tableNames();
        assertThat(tableNames).containsExactly(tableName);
    }

    @Test
    void putItemTest() {
        String tableName = "TestTable";
        createTable(tableName);
        // Insert an item into the table
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(Collections.singletonMap("id", AttributeValue.builder().s("123").build()))
                .build();

        dynamoDbClient.putItem(putItemRequest);

        // Retrieve the item from the table
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap("id", AttributeValue.builder().s("123").build()))
                .build();

        GetItemResponse getItemResponse = dynamoDbClient.getItem(getItemRequest);

        // Verify that the item is retrieved successfully
        assertTrue(getItemResponse.hasItem(), "Item should be retrieved from DynamoDB");
        logger.info("Retrieved item: {}", getItemResponse.item());
    }

    private void createTable(String tableName) {
        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();

        dynamoDbClient.createTable(createTableRequest);

        // Wait for the table to be created
        waitForTableToBecomeAvailable(dynamoDbClient, tableName);
        assertThat(dynamoDbClient.listTables().tableNames()).isNotEmpty();
    }

    private void waitForTableToBecomeAvailable(DynamoDbClient dynamoDbClient, String tableName) {
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();

        boolean isTableAvailable = false;
        Duration maxWaitingTime = Duration.ofMinutes(1);
        LocalDateTime startTime = LocalDateTime.now();
        while (!isTableAvailable && startTime.plus(maxWaitingTime).isAfter(LocalDateTime.now())) {
            try {
                DescribeTableResponse describeTableResponse = dynamoDbClient.describeTable(describeTableRequest);
                if (describeTableResponse.table().tableStatus() == TableStatus.ACTIVE) {
                    isTableAvailable = true;
                }
                Thread.sleep(1000);  // Wait for 1 second before retrying
            } catch (ResourceNotFoundException e) {
                // The table is not found, so it may not exist yet, continue retrying
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
