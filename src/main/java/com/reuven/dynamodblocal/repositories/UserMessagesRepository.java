package com.reuven.dynamodblocal.repositories;

import com.reuven.dynamodblocal.dto.PaginatedResult;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse1;
import com.reuven.dynamodblocal.entities.UserMessages;
import org.springframework.stereotype.Repository;
import org.springframework.web.util.UriUtils;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.reuven.dynamodblocal.utils.Constants.*;

@Repository
public class UserMessagesRepository extends BaseRepository<UserMessages> {

    private static final String DELIMITER = "#";
    private static final Map<String, String> EXPRESSION_ATTRIBUTE_NAME_MAP = Map.of(
            DELIMITER + MESSAGE.toLowerCase(), MESSAGE
    );
    private static final String PROJECTION_EXPRESSION_VALUES = String.join(", ", EXPRESSION_ATTRIBUTE_NAME_MAP.keySet());

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

    public PaginatedResult<UserMessages> getUserMessages(String userId,
                                                         LocalDateTime createdTimeBefore,
                                                         Integer limit,
                                                         Map<String, AttributeValue> exclusiveStartKey) {
        logger.info("Fetching paginated records...");
        AttributeValue attributeValue1 = AttributeValue.builder().s(createdTimeBefore.toString()).build();
        AttributeValue attributeValue2 = AttributeValue.fromS(createdTimeBefore.format(DateTimeFormatter.ISO_DATE_TIME));

        QueryEnhancedRequest.Builder queryRequestBuilder = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional
                        .sortLessThanOrEqualTo(Key.builder()
                                .partitionValue(userId)
                                .sortValue(attributeValue1)
                                .build())
                )
//                .consistentRead(false)
                .scanIndexForward(false)
                .limit(limit);

        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            if (!exclusiveStartKey.containsKey(USER_ID) || !exclusiveStartKey.containsKey(CREATED_TIME) || !exclusiveStartKey.containsKey(MESSAGE_UUID)) {
                throw new IllegalArgumentException("Invalid exclusiveStartKey provided");
            }
            queryRequestBuilder.exclusiveStartKey(exclusiveStartKey);
        }

        SdkIterable<Page<UserMessages>> pages = dynamoDbTableIndex.query(queryRequestBuilder.build());
        //lazy-loading
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

    public UserMessagesPageResponse getUserMessagesPage(String userId, Integer limit, Map<String, AttributeValue> exclusiveStartKey) {
        Map<String, Condition> conditionsMap = Map.of(
                USER_ID, Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.builder().s(userId).build())
                        .build());
        QueryResponse queryResponse = getQueryResponse(exclusiveStartKey, limit, conditionsMap);
        Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
        logger.info("LastEvaluatedKey: {}", lastEvaluatedKey);
        List<UserMessages> userMessages = toList(queryResponse.items());
        return new UserMessagesPageResponse(userMessages, lastEvaluatedKey);
    }

    public UserMessagesPageResponse1 getUserMessagesPage(String userId, Integer limit, String page) {
        Map<String, Condition> conditionsMap = Map.of(
                USER_ID, Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.builder().s(userId).build())
                        .build());
        Map<String, AttributeValue> exclusiveStartKey = buildExclusiveStartKey(userId, page);
        QueryResponse queryResponse = getQueryResponse(exclusiveStartKey, limit, conditionsMap);
        Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
        String nextPage = buildNextPageResult(lastEvaluatedKey);
        logger.info("nextPage: {}", nextPage);
        List<UserMessages> userMessages = toList(queryResponse.items());
        return new UserMessagesPageResponse1(userMessages, nextPage);
    }

    private static String buildNextPageResult(Map<String, AttributeValue> lastEvaluatedKey) {
        if (lastEvaluatedKey.isEmpty()) {
            return null;
        }
        String messageUuid = lastEvaluatedKey.get(MESSAGE_UUID).s();
        String createdTime = lastEvaluatedKey.get(CREATED_TIME).s();
        return createdTime + UriUtils.encode(DELIMITER, StandardCharsets.UTF_8) + messageUuid;
    }

    private Map<String, AttributeValue> buildExclusiveStartKey(String userId, String page) {
        if (page == null) {
            return null;
        }
        String pageDecode = UriUtils.decode(page, StandardCharsets.UTF_8);
        String[] pageParts = pageDecode.split(DELIMITER);
        if (pageParts.length != 2) {
            String message = String.format("Invalid nextPage format for userId=%s, must be in the format: %s%s%s", userId, CREATED_TIME, DELIMITER, MESSAGE_UUID);
            logger.error(message);
            throw new IllegalArgumentException(MESSAGE);
        }
        String createdTime = pageParts[0];
        String messageUuid = pageParts[1];
        return Map.of(
                USER_ID, AttributeValue.builder().s(userId).build(),
                MESSAGE_UUID, AttributeValue.builder().s(messageUuid).build(),
                CREATED_TIME, AttributeValue.builder().s(createdTime).build()
        );
    }


    private QueryResponse getQueryResponse(Map<String, AttributeValue> exclusiveStartKey, Integer limit, Map<String, Condition> conditionMap) {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(UserMessages.class.getSimpleName())
                .indexName(UserMessages.class.getSimpleName() + "Index")
                .limit(limit)
                .exclusiveStartKey(exclusiveStartKey)
                .keyConditions(conditionMap)
//                .projectionExpression(PROJECTION_EXPRESSION_VALUES)
//                .expressionAttributeNames(EXPRESSION_ATTRIBUTE_NAME_MAP)
//                .consistentRead(false)
                .scanIndexForward(false) //false=desc, true=asc //https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestSyntax
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
        logger.info("LastEvaluatedKey: {}", lastEvaluatedKey);
        return queryResponse;
    }

    public QueryResponse getUserMessages(String userId, LocalDateTime createdTimeBefore, Map<String, AttributeValue> exclusiveStartKey, Integer limit) {
        Map<String, Condition> conditionsMap = Map.of(
                USER_ID, Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.builder().s(userId).build())
                        .build(),
                CREATED_TIME, Condition.builder()
                        .comparisonOperator(ComparisonOperator.LE)
                        .attributeValueList(AttributeValue.builder().s(createdTimeBefore.format(DateTimeFormatter.ISO_DATE_TIME)).build())
                        .build());
        return getQueryResponse(exclusiveStartKey, limit, conditionsMap);
    }

}

