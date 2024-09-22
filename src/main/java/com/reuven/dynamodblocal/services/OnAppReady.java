package com.reuven.dynamodblocal.services;

import com.reuven.dynamodblocal.dto.PaginatedResult;
import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.repositories.UserMessagesRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

@Service
public class OnAppReady implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger logger = LogManager.getLogger(OnAppReady.class);

    private final DynamoDbClient dynamoDbClient;
    private final UserMessagesRepository userMessagesRepository;

    public OnAppReady(DynamoDbClient dynamoDbClient, UserMessagesRepository userMessagesRepository) {
        this.dynamoDbClient = dynamoDbClient;
        this.userMessagesRepository = userMessagesRepository;
    }

    @Override
    public void onApplicationEvent(final ApplicationReadyEvent readyEvent) {
//        dynamoDbClient.listTables().tableNames().forEach(System.out::println);
//        userMessagesRepository.save(new UserMessages("2", "message1"));
//        userMessagesRepository.save(new UserMessages("2", "message2"));
//        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
//        LocalDateTime time = LocalDateTime.of(2024, 9, 18, 0, 0);
//        List<UserMessages> userMessages = userMessagesRepository.getUserMessages("1", now);
//        List<UserMessages> userMessages1 = userMessagesRepository.getUserMessages("1", time);
//        List<UserMessages> userMessages2 = userMessagesRepository.getUserMessages("1", time);
//
//        List<UserMessages> userMessages3 = userMessagesRepository.getUserMessages("1", now, 2);
//        List<UserMessages> userMessages4 = userMessagesRepository.getUserMessages("1", time, 1);
//        List<UserMessages> userMessages5 = userMessagesRepository.getUserMessages("1", time);
//
//
//        Map<String, AttributeValue> lastEvaluatedKey1 = Map.of(
//                "UserId", AttributeValue.builder().s("1").build(),
//                "CreatedTime", AttributeValue.builder().s(time.toString()).build()
//        );
//
//        PaginatedResult<UserMessages> userMessages6;
//        do {
//            userMessages6 = userMessagesRepository.getUserMessages("1", time, 1, lastEvaluatedKey1);
//            logger.info("userMessages6 - items: {}", userMessages6.items());
//            lastEvaluatedKey1 = userMessages6.lastEvaluatedKey();
//            logger.info("LastEvaluatedKey: {}", lastEvaluatedKey1);
//            userMessages6.items().forEach(System.out::println);
//        } while (userMessages6.lastEvaluatedKey() != null);
//
//
//        Map<String, AttributeValue> lastEvaluatedKey = null;
//        QueryResponse queryResponse;
//        do {
//            queryResponse = userMessagesRepository.getUserMessages("1", lastEvaluatedKey, 1);
//            lastEvaluatedKey = queryResponse.lastEvaluatedKey();
//            queryResponse.items().forEach(System.out::println);
//        } while (queryResponse.hasLastEvaluatedKey());
    }


}
