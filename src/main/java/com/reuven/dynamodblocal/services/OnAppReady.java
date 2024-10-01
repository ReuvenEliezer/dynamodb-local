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
    }


}
