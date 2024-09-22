package com.reuven.dynamodblocal;

import com.reuven.dynamodblocal.dto.PaginatedResult;
import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.repositories.UserMessagesRepository;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class DynamodbLocalApplicationTests {

    private static final Logger logger = LogManager.getLogger(DynamodbLocalApplicationTests.class);


    @Autowired
    private DynamoDbClient dynamoDbClient;

    @Autowired
    private DynamoDbEnhancedClient dynamoDbEnhancedClient;

    @Autowired
    private UserMessagesRepository userMessagesRepository;

    private static DynamoDbTable<UserMessages> userMessagesTable;

    @PostConstruct
    void init() {
        userMessagesTable = dynamoDbEnhancedClient.table(UserMessages.class.getSimpleName(), TableSchema.fromBean(UserMessages.class));
    }

//    private static final GenericContainer<?> dynamoDbLocal = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local"))
//            .withExposedPorts(8000);
//

    static Process pr;

    @BeforeAll
    static void setUp() throws IOException {
        Runtime rt = Runtime.getRuntime();
        pr = rt.exec("docker compose -f docker-compose.yml up -d --wait");
//        dynamoDbLocal.start();
    }

    @AfterAll
    static void tearDown() {
        pr.destroy();
//        dynamoDbLocal.stop();
    }

    @BeforeEach
    void cleanUp() {
        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> scan = userMessagesTable.scan(scanRequest);
        for (UserMessages userMessages : scan.items()) {
            userMessagesTable.deleteItem(userMessages);
        }
    }


    @Test
    void saveElementTest() {
        userMessagesRepository.save(new UserMessages("2", "message1"));
        List<UserMessages> userMessages = userMessagesRepository.getUserMessages("2", LocalDateTime.now(ZoneOffset.UTC));
        assertThat(userMessages).isNotEmpty();
        assertThat(userMessages).hasSize(1);
    }

    @Test
    void sortTest() {
        dynamoDbClient.listTables().tableNames().forEach(System.out::println);
        userMessagesRepository.save(new UserMessages("2", "message1"));
        userMessagesRepository.save(new UserMessages("2", "message2"));
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        LocalDateTime time = LocalDateTime.of(2024, 9, 18, 0, 0);
        List<UserMessages> userMessages = userMessagesRepository.getUserMessages("1", now);
        List<UserMessages> userMessages1 = userMessagesRepository.getUserMessages("1", time);
        List<UserMessages> userMessages2 = userMessagesRepository.getUserMessages("1", time);

        List<UserMessages> userMessages3 = userMessagesRepository.getUserMessages("1", now, 2);
        List<UserMessages> userMessages4 = userMessagesRepository.getUserMessages("1", time, 1);
        List<UserMessages> userMessages5 = userMessagesRepository.getUserMessages("1", time);


        Map<String, AttributeValue> lastEvaluatedKey1 = Map.of(
                "UserId", AttributeValue.builder().s("1").build(),
                "CreatedTime", AttributeValue.builder().s(time.toString()).build()
        );

        PaginatedResult<UserMessages> userMessages6;
        do {
            userMessages6 = userMessagesRepository.getUserMessages("1", time, 1, lastEvaluatedKey1);
            logger.info("userMessages6 - items: {}", userMessages6.items());
            lastEvaluatedKey1 = userMessages6.lastEvaluatedKey();
            logger.info("LastEvaluatedKey: {}", lastEvaluatedKey1);
            userMessages6.items().forEach(System.out::println);
        } while (userMessages6.lastEvaluatedKey() != null);


        Map<String, AttributeValue> lastEvaluatedKey = null;
        QueryResponse queryResponse;
        do {
            queryResponse = userMessagesRepository.getUserMessages("1", lastEvaluatedKey, 1);
            lastEvaluatedKey = queryResponse.lastEvaluatedKey();
            queryResponse.items().forEach(System.out::println);
        } while (queryResponse.hasLastEvaluatedKey());
    }

}
