package com.reuven.dynamodblocal.repositories;

import com.reuven.dynamodblocal.dto.PaginatedResult;
import com.reuven.dynamodblocal.entities.UserMessages;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
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
public class UserMessagesRepository extends BaseRepository<UserMessages> {

    public UserMessagesRepository(DynamoDbClient dynamoDbClient, DynamoDbEnhancedClient dynamoDbEnhancedClient) {
        super(dynamoDbClient, dynamoDbEnhancedClient, UserMessages.class);
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

        PageIterable<UserMessages> pages = dynamoDbTable.query(queryRequest);

        pages.stream().forEach(page -> {
            printUserMessage(page.items());
            logger.info("LastEvaluatedKey: {}", page.lastEvaluatedKey());
        });
        return pages.stream()
                .flatMap(page -> page.items().stream())
                .toList();
    }

    private void printUserMessage(List<UserMessages> userMessages) {
        userMessages.forEach(u -> logger.info("UserId: {}, CreatedTime: {}, Message: {}", u.getUserId(), u.getCreatedTime(), u.getMessage()));
    }

    public List<UserMessages> getUserMessages(String userId) {
        return getUserMessages(userId, null);
    }

    public PaginatedResult<UserMessages> getUserMessages(String userId, LocalDateTime createdTimeBefore, Integer limit,
                                                         Map<String, AttributeValue> exclusiveStartKey) {
        logger.info("Fetching paginated records...");
        QueryEnhancedRequest.Builder queryRequestBuilder = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional
                        .sortLessThanOrEqualTo(Key.builder()
                                .partitionValue(userId)
                                .sortValue(createdTimeBefore.format(DateTimeFormatter.ISO_DATE_TIME))
                                .build())
                )
//                .consistentRead(false)
                .scanIndexForward(false)
                .limit(limit);

        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            if (!exclusiveStartKey.containsKey("UserId") || !exclusiveStartKey.containsKey("CreatedTime") || !exclusiveStartKey.containsKey("MessageUuid")) {
                throw new IllegalArgumentException("Invalid exclusiveStartKey provided");
            }
            queryRequestBuilder.exclusiveStartKey(exclusiveStartKey);
        }

//        SdkIterable<Page<UserMessages>> scan = userMessagesTableIndex.scan();

        SdkIterable<Page<UserMessages>> pages = dynamoDbTableIndex.query(queryRequestBuilder.build());
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
        Map<String, AttributeValue> lastEvaluatedKey = page.lastEvaluatedKey();
        List<UserMessages> items = page.items();
        return new PaginatedResult<>(items, lastEvaluatedKey);
    }


    public QueryResponse getUserMessages(String userId, Map<String, AttributeValue> exclusiveStartKey, Integer limit) {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(UserMessages.class.getSimpleName())
                .keyConditionExpression("UserId = :userId")
                .expressionAttributeValues(Map.of(":userId", AttributeValue.builder().s(userId).build()))
                .limit(limit)
                .exclusiveStartKey(exclusiveStartKey)
                .scanIndexForward(false)
//                .consistentRead(false)
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

        Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
        logger.info("LastEvaluatedKey: {}", lastEvaluatedKey);

        return queryResponse;
    }

}

