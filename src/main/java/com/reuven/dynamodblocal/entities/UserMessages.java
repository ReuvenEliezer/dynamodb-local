package com.reuven.dynamodblocal.entities;

import com.reuven.dynamodblocal.dto.LocalDateTimeConverter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
//@DynamoDbBean
//public record UserMessages(@DynamoDbPartitionKey @DynamoDbAttribute("UserId") String userId, // מפתח Hash Key
//                           @DynamoDbSortKey @DynamoDbAttribute("CreatedTime") @DynamoDbConvertedBy(LocalDateTimeConverter.class) LocalDateTime createdTime,// Sort Key כ-String
//                           @DynamoDbAttribute("Message") String message) {
//    public UserMessages(String userId, String message) {
//        this(userId, LocalDateTime.now(ZoneOffset.UTC), message);
//    }
//
//    // Default constructor (Required)
//    public UserMessages() {
//        this(null, null, null);
//    }
//}

@DynamoDbBean
public class UserMessages {
    private String userId; // Partition Key
    private LocalDateTime createdTime; // Sort Key (stored as String in ISO-8601 format)
    private String message;

    // Default constructor (Required)
    public UserMessages() {}

    public UserMessages(String userId, String message) {
        this.userId = userId;
        this.createdTime = LocalDateTime.now(ZoneOffset.UTC); //.format(DateTimeFormatter.ISO_DATE_TIME);
        this.message = message;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("UserId")
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute("CreatedTime")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(LocalDateTime createdTime) {
        this.createdTime = createdTime;
    }

    @DynamoDbAttribute("Message")
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserMessages that = (UserMessages) o;
        return Objects.equals(userId, that.userId) && Objects.equals(createdTime, that.createdTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, createdTime);
    }

    @Override
    public String toString() {
        return "UserMessages{" +
                "userId='" + userId + '\'' +
                ", createdTime=" + createdTime +
                ", message='" + message + '\'' +
                '}';
    }
}
