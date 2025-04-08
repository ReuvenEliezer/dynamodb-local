package com.reuven.dynamodblocal;

import com.reuven.dynamodblocal.dto.PaginatedResult;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse1;
import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.entities.UserMetadata;
import com.reuven.dynamodblocal.repositories.UserMessagesRepository;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static com.reuven.dynamodblocal.utils.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @BeforeEach
    void cleanUp() {
        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        userMessagesRepository.delete(pages.items().stream().toList());
//        for (UserMessages userMessages : pages.items()) {
//            userMessagesTable.deleteItem(userMessages);
//        }
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
        userMessagesRepository.save(createUserMessage(userId, MESSAGE, now));
        List<UserMessages> userMessages = userMessagesRepository.getUserMessages(userId);
        assertThat(userMessages).isNotEmpty();
        assertThat(userMessages).hasSize(1);
    }

    @Test
    void saveElementWithConverterTest() {
        String userId = "1";
        UserMessages userMessages = new UserMessages(userId, MESSAGE, new UserMetadata("email@gmail.com", "address"));
        userMessagesRepository.save(userMessages);
        List<UserMessages> userMessagesSaved = userMessagesRepository.getUserMessages(userId);
        assertThat(userMessagesSaved).isNotEmpty();
        assertThat(userMessagesSaved).hasSize(1);
        assertThat(userMessagesSaved.get(0).getUserMetadata()).isEqualTo(userMessages.getUserMetadata());
    }

    @Test
    void booleanItemTest() {
        String userId = "1";
        UserMessages userMessages = new UserMessages(userId, MESSAGE, new UserMetadata("email@gmail.com", "address"));
        userMessages.setRead(Boolean.TRUE);
        userMessagesRepository.save(userMessages);
        List<UserMessages> userMessagesSaved = userMessagesRepository.getUserMessages(userId);
        assertThat(userMessagesSaved).isNotEmpty();
        assertThat(userMessagesSaved).hasSize(1);
        assertThat(userMessagesSaved.getFirst().getRead()).isTrue();
    }

    @Test
    void booleanNullValueItemTest() {
        String userId = "1";
        UserMessages userMessages = new UserMessages(userId, MESSAGE, new UserMetadata("email@gmail.com", "address"));
        userMessagesRepository.save(userMessages);
        List<UserMessages> userMessagesSaved = userMessagesRepository.getUserMessages(userId);
        assertThat(userMessagesSaved).isNotEmpty();
        assertThat(userMessagesSaved).hasSize(1);
        assertThat(userMessagesSaved.get(0).getRead()).isNull();
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
    }

    @Test
    void attMapToObjTest() {
        String userId1 = "1";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        List<UserMessages> userMessages = List.of(
                createUserMessage(userId1, "message1", now.minusDays(5)),
                createUserMessage(userId1, "message2", now.minusDays(4)),
                createUserMessage(userId1, "message3", now.minusDays(2)),
                createUserMessage(userId1, "message4", now.minusDays(6))
        );
        userMessagesRepository.save(userMessages);

        List<UserMessages> userMessagesSaved = userMessagesRepository.getUserMessages(userId1);
        Map<String, AttributeValue> stringAttributeValueMap = BeanTableSchema.create(UserMessages.class)
                .itemToMap(userMessagesSaved.get(0), true);
        List<Map<String, AttributeValue>> userMessagesSavedAttMap = userMessagesSaved.stream()
                .map(userMessages1 ->
                        BeanTableSchema.create(UserMessages.class).itemToMap(userMessages1, true))
                .toList();
        List<UserMessages> userMessagesConverted = userMessagesSavedAttMap.stream()
                .map(attMap ->
                        BeanTableSchema.create(UserMessages.class).mapToItem(attMap))
                .toList();

        assertThat(userMessagesConverted).isEqualTo(userMessagesSaved);
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

        Map<String, AttributeValue> exclusiveStartKey = Map.of(
                USER_ID, AttributeValue.builder().s(userId1).build(),
                CREATED_TIME, AttributeValue.builder().s(now.toString()).build(),
                MESSAGE_UUID, AttributeValue.builder().s("dummy").build() // dummy value not take in the index key but must provide in map
        );

        PaginatedResult<UserMessages> userMessages;
        final int limit = 1;
        int count = 0;
        do {
            userMessages = userMessagesRepository.getUserMessages(userId1, now.plusDays(1), limit, exclusiveStartKey);
            logger.info("userMessages - items: {}", userMessages.items());
            exclusiveStartKey = userMessages.lastEvaluatedKey();
            logger.info("LastEvaluatedKey: {}", exclusiveStartKey);
            userMessages.items().forEach(System.out::println);
            if (userMessages.lastEvaluatedKey() != null) {
                count++;
            }
        } while (userMessages.lastEvaluatedKey() != null);

        assertThat(count).isEqualTo(2);


        count = 0;
        do {
            userMessages = userMessagesRepository.getUserMessages(userId1, now.minusDays(1), limit, exclusiveStartKey);
            logger.info("userMessages - items: {}", userMessages.items());
            exclusiveStartKey = userMessages.lastEvaluatedKey();
            logger.info("LastEvaluatedKey: {}", exclusiveStartKey);
            userMessages.items().forEach(System.out::println);
            if (userMessages.lastEvaluatedKey() != null) {
                count++;
            }
        } while (userMessages.lastEvaluatedKey() != null);

        assertThat(count).isEqualTo(1);
    }

    @Test
    void paginationByQueryResponseSdkSortedByCreatedDateDESCTest() {
        String userId1 = "1";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        List<UserMessages> userMessages = List.of(
                createUserMessage(userId1, "message1", now.minusDays(5)),
                createUserMessage(userId1, "message2", now.minusDays(4)),
                createUserMessage(userId1, "message3", now.minusDays(2)),
                createUserMessage(userId1, "message4", now.minusDays(6))
        );
        userMessagesRepository.save(userMessages);

        List<UserMessages> userMessagesSaved = userMessagesRepository.getUserMessages(userId1);
        Map<String, AttributeValue> stringAttributeValueMap = BeanTableSchema.create(UserMessages.class)
                .itemToMap(userMessagesSaved.get(0), true);
        userMessagesSaved.stream().map(userMessages1 -> BeanTableSchema.create(UserMessages.class).itemToMap(userMessages1, true))
                .forEach(System.out::println);
        assertThat(userMessagesSaved).hasSize(userMessages.size());
        assertThat(userMessagesSaved)
                .satisfies(list -> {
                    boolean isSorted = list.equals(
                            list.stream()
                                    .sorted(Comparator.comparing(UserMessages::getCreatedTime).reversed())
                                    .toList());
                    assertThat(isSorted).isFalse();
                });

        Map<String, AttributeValue> exclusiveStartKey = null;
        UserMessagesPageResponse userMessagesPage;
        List<UserMessages> userMessagesResult = new ArrayList<>(userMessages.size());
        do {
            userMessagesPage = userMessagesRepository.getUserMessagesPage(userId1, 1, exclusiveStartKey);
            exclusiveStartKey = userMessagesPage.nextPage();
            userMessagesPage.userMessages().forEach(System.out::println);
            userMessagesResult.addAll(userMessagesPage.userMessages());
        } while (!exclusiveStartKey.isEmpty());
        assertThat(userMessagesResult).hasSize(userMessages.size());
        assertThat(userMessagesResult).isSortedAccordingTo(Comparator.comparing(UserMessages::getCreatedTime).reversed());

        List<UserMessages> userMessagesResultSimplePage = new ArrayList<>(userMessages.size());
        String page = null;
        UserMessagesPageResponse1 userMessagesPageResponse1;
        do {
            userMessagesPageResponse1 = userMessagesRepository.getUserMessagesPage(userId1, 1, page);
            page = userMessagesPageResponse1.nextPage();
            userMessagesPageResponse1.userMessages().forEach(System.out::println);
            userMessagesResultSimplePage.addAll(userMessagesPageResponse1.userMessages());
        } while (page != null);
        assertThat(userMessagesResultSimplePage).hasSize(userMessages.size());
        assertThat(userMessagesResultSimplePage).isSortedAccordingTo(Comparator.comparing(UserMessages::getCreatedTime).reversed());
    }


    @Test
    void paginationByQueryResponseSdkTest() {
        String userId1 = "1";
        String userId2 = "2";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        userMessagesRepository.save(List.of(
                createUserMessage(userId1, "message1", now),
                createUserMessage(userId1, "message2", now),
                createUserMessage(userId2, "message1", now),
                createUserMessage(userId2, "message2", now)
        ));

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        assertThat(pages.items()).hasSize(4);


        Map<String, AttributeValue> exclusiveStartKey = null;
        QueryResponse queryResponse;
        int count = 0;
        do {
            queryResponse = userMessagesRepository.getUserMessages(userId1, now.plusDays(1), exclusiveStartKey, 1);
            exclusiveStartKey = queryResponse.lastEvaluatedKey();
            queryResponse.items().forEach(System.out::println);
            if (queryResponse.hasLastEvaluatedKey()) {
                count++;
            }
        } while (queryResponse.hasLastEvaluatedKey());

        assertThat(count).isEqualTo(2);

        exclusiveStartKey = null;
        count = 0;
        do {
            queryResponse = userMessagesRepository.getUserMessages(userId1, now.minusDays(1), exclusiveStartKey, 1);
            exclusiveStartKey = queryResponse.lastEvaluatedKey();
            queryResponse.items().forEach(System.out::println);
            if (queryResponse.hasLastEvaluatedKey()) {
                count++;
            }
        } while (queryResponse.hasLastEvaluatedKey());

        assertThat(count).isEqualTo(0);
    }

    @Test
    void transactionRollBackTest() {
        String userId1 = "1";
        String userId2 = "2";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        UUID uuid = UUID.randomUUID();

        assertThatThrownBy(() -> userMessagesRepository.saveInTransact(List.of(
                createUserMessage(userId2, "message1", now), //valid
                createUserMessage(userId1, "message1", now), //valid
                createUserMessage(userId1, "message2", now, uuid), //valid
                createUserMessage(userId1, "message3", now, uuid) // duplicated (key & sort key) of message2
        )))
                .isInstanceOf(DynamoDbException.class)
                .hasMessageContaining("Transaction request cannot include multiple operations on one item");

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        assertThat(pages.items()).isEmpty();
    }

    @Test
    @Disabled
    void batchWithNoTransactionalTest() {
        String userId1 = "1";
        String userId2 = "2";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        UUID uuid = UUID.randomUUID();

        assertThatThrownBy(() -> {
            BatchWriteResult batchWriteResult = userMessagesRepository.save(List.of(
                    createUserMessage(userId2, "message1", now), //valid
                    createUserMessage(userId1, "message1", now), //valid
                    createUserMessage(userId1, "message2", now, uuid), //valid
                    createUserMessage(userId1, "message3", now, uuid) // duplicated (same key & sort key) of userId1-message2
            ));
        }).isInstanceOf(DynamoDbException.class)
                .hasMessageContaining("Provided list of item keys contains duplicates");

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        assertThat(pages.items()).hasSize(3);
    }


    @Test
    @Disabled
    void saveWithNonDuplicateErrorTest() {
        String userId1 = "1";
        String userId2 = "2";
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        UserMessages validMessage1 = createUserMessage(userId1, "message1", now);
        UserMessages invalidMessage = new UserMessages(null, "message2");
//        UserMessages invalidMessage = createUserMessage(null, "message2", null);
        UserMessages validMessage2 = createUserMessage(userId2, "message3", now);

        assertThatThrownBy(() ->
                userMessagesRepository.save(List.of(validMessage1, invalidMessage, validMessage2))
        )
                .isInstanceOf(DynamoDbException.class)
                .hasMessageContaining("One of the required keys was not given a value");

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();
        PageIterable<UserMessages> pages = userMessagesTable.scan(scanRequest);
        assertThat(pages.items()).hasSize(2);
    }


    private UserMessages createUserMessage(String userId, String message, LocalDateTime createdTime) {
        UserMessages userMessages = new UserMessages(userId, message);
        setField(userMessages, "createdTime", createdTime);
        return userMessages;
    }

    private UserMessages createUserMessage(String userId, String message, LocalDateTime createdTime, UUID uuid) {
        UserMessages userMessage = createUserMessage(userId, message, createdTime);
        setField(userMessage, "messageUuid", uuid.toString());
        return userMessage;
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
