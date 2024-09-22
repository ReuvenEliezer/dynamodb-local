package com.reuven.dynamodblocal.repositories;

import com.reuven.dynamodblocal.dto.PaginatedResult;
import com.reuven.dynamodblocal.entities.UserMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@Repository
public class UserMessagesRepository {

    private static final Logger logger = LogManager.getLogger(UserMessagesRepository.class);

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    private final DynamoDbTable<UserMessages> userMessagesTable;
    private final DynamoDbIndex<UserMessages> userMessagesTableIndex;
    private final DynamoDbClient dynamoDbClient;


    public UserMessagesRepository(DynamoDbClient dynamoDbClient, DynamoDbEnhancedClient dynamoDbEnhancedClient) {
        this.dynamoDbClient = dynamoDbClient;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
        this.userMessagesTable = dynamoDbEnhancedClient.table(UserMessages.class.getSimpleName(), TableSchema.fromBean(UserMessages.class));
        this.userMessagesTableIndex = userMessagesTable.index(UserMessages.class.getSimpleName() + "Index");
    }

    public void save(UserMessages userMessages) {
        userMessagesTable.putItem(userMessages);
    }

    public void delete(UserMessages userMessages) {
        userMessagesTable.deleteItem(userMessages);
    }

    public void save(List<UserMessages> userMessagesList) {
        if (userMessagesList.isEmpty()) {
            return;
        }
        doInBatch(writeBatchBuilder -> userMessagesList.forEach(writeBatchBuilder::addPutItem));
    }

    public void delete(List<UserMessages> userMessagesList) {
        if (userMessagesList.isEmpty()) {
            return;
        }
        doInBatch(writeBatchBuilder -> userMessagesList.forEach(writeBatchBuilder::addDeleteItem));
    }

    private void doInBatch(Consumer<WriteBatch.Builder<UserMessages>> action) {
        WriteBatch.Builder<UserMessages> writeBatchBuilder = WriteBatch.builder(UserMessages.class)
                .mappedTableResource(userMessagesTable);

        action.accept(writeBatchBuilder);

        BatchWriteItemEnhancedRequest batchWriteRequest = BatchWriteItemEnhancedRequest.builder()
                .addWriteBatch(writeBatchBuilder.build())
                .build();

        dynamoDbEnhancedClient.batchWriteItem(batchWriteRequest);
    }

    public List<UserMessages> getUserMessages(String userId, Integer limit) {
        logger.info("sorted and limit");
        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(
                        Key.builder()
                                .partitionValue(userId)
                                .build()))
                .limit(limit)
                .build();

        PageIterable<UserMessages> pages = userMessagesTable.query(queryRequest);

        pages.stream().forEach(page -> {
            printUserMessage(page.items());
            logger.info("LastEvaluatedKey: {}", page.lastEvaluatedKey());
        });
        return pages.stream()
                .flatMap(page -> page.items().stream())
                .toList();
    }

    private static void printUserMessage(List<UserMessages> userMessages) {
        userMessages.forEach(u -> {
            logger.info("UserId: {}", u.getUserId());
            logger.info("CreatedTime: {}", u.getCreatedTime());
            logger.info("Message: {}", u.getMessage());
        });
    }

    public List<UserMessages> getUserMessages(String userId) {
        return getUserMessages(userId, null);
    }

    public PaginatedResult<UserMessages> getUserMessages(String userId, LocalDateTime createdTimeBefore, Integer limit,
                                                         Map<String, AttributeValue> lastEvaluatedKey) {
        logger.info("Fetching paginated records...");
        QueryEnhancedRequest.Builder queryRequestBuilder = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional
                        .sortLessThanOrEqualTo(Key.builder()
                                .partitionValue(userId)
                                .sortValue(createdTimeBefore.format(DateTimeFormatter.ISO_DATE_TIME))
                                .build())
                )
                .scanIndexForward(false)
                .limit(limit);

        if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
            if (!lastEvaluatedKey.containsKey("UserId") || !lastEvaluatedKey.containsKey("CreatedTime") || !lastEvaluatedKey.containsKey("MessageUuid")) {
                throw new IllegalArgumentException("Invalid lastEvaluatedKey provided");
            }
            queryRequestBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

//        SdkIterable<Page<UserMessages>> scan = userMessagesTableIndex.scan();

        SdkIterable<Page<UserMessages>> pages = userMessagesTableIndex.query(queryRequestBuilder.build());
        for (Page<UserMessages> page : pages) {
            printUserMessage(page.items());
            logger.info("LastEvaluatedKey: {}", page.lastEvaluatedKey());
        }
        Optional<Page<UserMessages>> firstPage = pages.stream().findFirst();

        if (firstPage.isEmpty() || firstPage.get().items().isEmpty()) {
            logger.info("no results found");
            return new PaginatedResult<>(Collections.emptyList(), null);
        }

        Page<UserMessages> page = firstPage.get();
        Map<String, AttributeValue> newLastEvaluatedKey = page.lastEvaluatedKey();
        List<UserMessages> items = page.items();
        return new PaginatedResult<>(items, newLastEvaluatedKey);
    }


    public QueryResponse getUserMessages(String userId, Map<String, AttributeValue> exclusiveStartKey, int limit) {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(UserMessages.class.getSimpleName())
                .keyConditionExpression("UserId = :userId")
                .expressionAttributeValues(Map.of(":userId", AttributeValue.builder().s(userId).build()))
                .limit(limit)
                .exclusiveStartKey(exclusiveStartKey)
                .scanIndexForward(false)
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

        Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
        logger.info("LastEvaluatedKey: {}", lastEvaluatedKey);

        return queryResponse;
    }

}

