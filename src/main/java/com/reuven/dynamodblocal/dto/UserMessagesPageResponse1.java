package com.reuven.dynamodblocal.dto;

import com.reuven.dynamodblocal.entities.UserMessages;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

public record UserMessagesPageResponse1(List<UserMessages> userMessages, String nextPage) {
}
