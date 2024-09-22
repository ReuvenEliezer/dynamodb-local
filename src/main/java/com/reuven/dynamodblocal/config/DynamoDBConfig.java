package com.reuven.dynamodblocal.config;

import com.reuven.dynamodblocal.entities.UserMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.net.URI;

@Configuration
public class DynamoDBConfig {

    private static final Logger logger = LogManager.getLogger(DynamoDBConfig.class);

    private static final String USER_MESSAGES_TABLE_NAME = UserMessages.class.getSimpleName();

    //https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html#docker
    @Bean
    public DynamoDbClient dynamoDbClient() {
        String assessKey = "dummyKey";
        String secretKey = "dummySecret";
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(assessKey, secretKey);
        DynamoDbClient amazonDynamoDB = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_WEST_2)
                .build();
        createTable(amazonDynamoDB, USER_MESSAGES_TABLE_NAME, "UserId", "MessageUuid");
        return amazonDynamoDB;
    }

    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }


    public void createTable(DynamoDbClient ddb, String tableName, String partitionKey, String sortKey) {
        if (ddb.listTables().tableNames().contains(tableName)) {
            return;
        }
        DynamoDbWaiter dbWaiter = ddb.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName(partitionKey)
                                .attributeType(ScalarAttributeType.S) // Partition key type
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName(sortKey)
                                .attributeType(ScalarAttributeType.S) // Sort key type
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("CreatedTime")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName(partitionKey)
                                .keyType(KeyType.HASH) // Partition key
                                .build(),
                        KeySchemaElement.builder()
                                .attributeName(sortKey)
                                .keyType(KeyType.RANGE) // Sort key
                                .build())
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                                .indexName(UserMessages.class.getSimpleName() + "Index")
                                .keySchema(
                                        KeySchemaElement.builder()
                                                .attributeName(partitionKey)
                                                .keyType(KeyType.HASH)
                                                .build(),
                                        KeySchemaElement.builder()
                                                .attributeName("CreatedTime")
                                                .keyType(KeyType.RANGE)
                                                .build())
                                .projection(Projection.builder()
                                        .projectionType(ProjectionType.ALL)
                                        .build())
                                .provisionedThroughput(buildProvisionedThroughput(5L, 5L))
                                .build())
                .provisionedThroughput(buildProvisionedThroughput(10L, 10L))
                .tableName(tableName)
                .build();

        CreateTableResponse response = ddb.createTable(request);
        DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();

        WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
        waiterResponse.matched().response().ifPresent(System.out::println);

        String tableId = response.tableDescription().tableName();
        logger.info("Created table '{}'", tableId);
    }

    private static ProvisionedThroughput buildProvisionedThroughput(Long readCapacityUnits, Long writeCapacityUnits) {
        return ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build();
    }


}
