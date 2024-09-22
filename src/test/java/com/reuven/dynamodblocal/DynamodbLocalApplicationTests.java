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
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.lang.reflect.Field;
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
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        for (UserMessages userMessages : pages.items()) {
            userMessagesTable.deleteItem(userMessages);
        }
    }

    @Test
    void tableExistsTest() {
        List<String> tableNames = dynamoDbClient.listTables().tableNames();
        assertThat(tableNames).containsExactlyInAnyOrder(UserMessages.class.getSimpleName());
    }

    @Test
    void indexExistsTest() {
        DescribeTableResponse describeTableResponse = dynamoDbClient.describeTable(DescribeTableRequest.builder()
                .tableName(UserMessages.class.getSimpleName())
                .build());

        assertThat(describeTableResponse.table().globalSecondaryIndexes())
                .extracting(GlobalSecondaryIndexDescription::indexName)
                .contains(UserMessages.class.getSimpleName() + "Index");
    }


    @Test
    void saveElementTest() {
        String userId = "1";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        userMessagesRepository.save(createUserMessage(userId, "message", now));
        List<UserMessages> userMessages = userMessagesRepository.getUserMessages(userId);
        assertThat(userMessages).isNotEmpty();
        assertThat(userMessages).hasSize(1);
    }

    @Test
    void userMessagesBatchSaveTest() {
        String userId1 = "1";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        LocalDateTime yesterday = now.minusDays(1);
        UserMessages user1Message1Today = createUserMessage(userId1, "message1", now);
        UserMessages user1Message2Today = createUserMessage(userId1, "message2", now.minusHours(1));
        UserMessages user1Message3Yesterday = createUserMessage(userId1, "message3", yesterday);
        UserMessages user1Message4Yesterday = createUserMessage(userId1, "message4", yesterday.minusHours(1));

        String userId2 = "2";
        UserMessages user2Message1Today = createUserMessage(userId2, "message1", now);
        UserMessages user2Message2Today = createUserMessage(userId2, "message2", now.minusHours(1));
        UserMessages user2Message3Yesterday = createUserMessage(userId2, "message3", yesterday);
        UserMessages user2Message4Yesterday = createUserMessage(userId2, "message4", yesterday.minusHours(1));

        userMessagesRepository.save(List.of(
                user1Message1Today, user1Message2Today, user1Message3Yesterday, user1Message4Yesterday,
                user2Message1Today, user2Message2Today, user2Message3Yesterday, user2Message4Yesterday
        ));

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        assertThat(pages.items()).hasSize(8);
    }

    @Test
    void userMessagesWithSameTimeTest() {
        String userId1 = "1";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        userMessagesRepository.save(List.of(
                createUserMessage(userId1, "message1", now),
                createUserMessage(userId1, "message2", now)
        ));

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        assertThat(pages.items()).hasSize(2);


        Map<String, AttributeValue> lastEvaluatedKey = null;
        QueryResponse queryResponse;
        do {
            queryResponse = userMessagesRepository.getUserMessages(userId1, lastEvaluatedKey, 1);
            lastEvaluatedKey = queryResponse.lastEvaluatedKey();
            queryResponse.items().forEach(System.out::println);
        } while (queryResponse.hasLastEvaluatedKey());
    }

    @Test
    void paginationTest() {
        String userId1 = "1";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        UserMessages user1Message1Now = createUserMessage(userId1, "message1", now);
        UserMessages user1Message2Now = createUserMessage(userId1, "message2", now.minusDays(1));

        String userId2 = "2";
        UserMessages user2Message1Now = createUserMessage(userId2, "message1", now);
        UserMessages user2Message2Now = createUserMessage(userId2, "message2", now.minusDays(1));

        userMessagesRepository.save(List.of(
                user1Message1Now, user1Message2Now,
                user2Message1Now, user2Message2Now
        ));

        Map<String, AttributeValue> lastEvaluatedKey = Map.of(
                "UserId", AttributeValue.builder().s(userId1).build(),
                "CreatedTime", AttributeValue.builder().s(now.toString()).build(),
                "MessageUuid", AttributeValue.builder().s("dummy").build() // dummy value not take in the index key but must provide in map
        );

        PaginatedResult<UserMessages> userMessages;
        int limit = 1;
        int count = 0;
        do {
            userMessages = userMessagesRepository.getUserMessages(userId1, now.plusDays(1), limit, lastEvaluatedKey);
            logger.info("userMessages - items: {}", userMessages.items());
            lastEvaluatedKey = userMessages.lastEvaluatedKey();
            logger.info("LastEvaluatedKey: {}", lastEvaluatedKey);
            userMessages.items().forEach(System.out::println);
            if (userMessages.lastEvaluatedKey() != null) {
                count++;
            }
        } while (userMessages.lastEvaluatedKey() != null);

        assertThat(count).isEqualTo(2);


        count = 0;
        do {
            userMessages = userMessagesRepository.getUserMessages(userId1, now.minusDays(1), limit, lastEvaluatedKey);
            logger.info("userMessages - items: {}", userMessages.items());
            lastEvaluatedKey = userMessages.lastEvaluatedKey();
            logger.info("LastEvaluatedKey: {}", lastEvaluatedKey);
            userMessages.items().forEach(System.out::println);
            if (userMessages.lastEvaluatedKey() != null) {
                count++;
            }
        } while (userMessages.lastEvaluatedKey() != null);

        assertThat(count).isEqualTo(1);
    }


    private UserMessages createUserMessage(String userId, String message, LocalDateTime createdTime) {
        UserMessages userMessages = new UserMessages(userId, message);
        setField(userMessages, "createdTime", createdTime);
        return userMessages;
    }


    private void setField(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

}
