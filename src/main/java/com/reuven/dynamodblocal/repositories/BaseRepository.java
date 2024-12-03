package com.reuven.dynamodblocal.repositories;

import com.reuven.dynamodblocal.entities.UserMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class BaseRepository<T> {

    protected final Logger logger = LogManager.getLogger(this.getClass());
    protected static final String INDEX = "Index";

    protected final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    protected final DynamoDbTable<T> dynamoDbTable;
    protected final DynamoDbIndex<T> dynamoDbTableIndex;
    protected final DynamoDbClient dynamoDbClient;
    protected final Class<T> clazz;


    public BaseRepository(DynamoDbClient dynamoDbClient, DynamoDbEnhancedClient dynamoDbEnhancedClient, Class<T> clazz) {
        this.dynamoDbClient = dynamoDbClient;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
        this.dynamoDbTable = dynamoDbEnhancedClient.table(clazz.getSimpleName(), TableSchema.fromBean(clazz));
        this.dynamoDbTableIndex = dynamoDbTable.index(clazz.getSimpleName() + INDEX);
        this.clazz = clazz;
    }

    public void save(T item) {
        dynamoDbTable.putItem(item);
    }

    public void delete(T item) {
        dynamoDbTable.deleteItem(item);
    }

    public BatchWriteResult save(List<T> list) {
        if (list.isEmpty()) {
            return null;
        }
        return doInBatch(writeBatchBuilder -> list.forEach(writeBatchBuilder::addPutItem));
    }

    public void saveInTransact(List<T> list) {
        if (list.isEmpty()) {
            return;
        }
        doInTransact(transactWriteBuilder -> list.forEach(item -> transactWriteBuilder.addPutItem(dynamoDbTable, item)));
    }

    public BatchWriteResult delete(List<T> list) {
        if (list.isEmpty()) {
            return null;
        }
        return doInBatch(writeBatchBuilder -> list.forEach(writeBatchBuilder::addDeleteItem));
    }

    public void deleteInTransact(List<T> list) {
        if (list.isEmpty()) {
            return;
        }
        doInTransact(transactWriteBuilder -> {
            for (T item : list) {
                transactWriteBuilder.addDeleteItem(dynamoDbTable, item);
            }
        });
    }

    private BatchWriteResult doInBatch(Consumer<WriteBatch.Builder<T>> action) {
        WriteBatch.Builder<T> writeBatchBuilder = WriteBatch.builder(clazz)
                .mappedTableResource(dynamoDbTable);

        action.accept(writeBatchBuilder);

        BatchWriteItemEnhancedRequest batchWriteRequest = BatchWriteItemEnhancedRequest.builder()
                .addWriteBatch(writeBatchBuilder.build())
                .build();

        return dynamoDbEnhancedClient.batchWriteItem(batchWriteRequest);
    }

    private void doInTransact(Consumer<TransactWriteItemsEnhancedRequest.Builder> action) {
        TransactWriteItemsEnhancedRequest.Builder transactWriteBuilder = TransactWriteItemsEnhancedRequest.builder();

        // Apply the action which could add multiple write operations (Put, Delete, etc.)
        action.accept(transactWriteBuilder);

        TransactWriteItemsEnhancedRequest transactWriteRequest = transactWriteBuilder.build();

        dynamoDbEnhancedClient.transactWriteItems(transactWriteRequest);
    }

    protected List<T> toList(List<Map<String, AttributeValue>> attMaps) {
        BeanTableSchema<T> beanTableSchema = TableSchema.fromBean(clazz);
        return attMaps.stream()
                .map(beanTableSchema::mapToItem)
                .toList();
    }

}
