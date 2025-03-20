package com.reuven.dynamodblocal.dto.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reuven.dynamodblocal.config.ObjectMapperConfig;
import com.reuven.dynamodblocal.entities.UserMetadata;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;


public class UserMetadataConverter implements AttributeConverter<UserMetadata> {

    private static final ObjectMapper objectMapper = ObjectMapperConfig.objectMapper();

    @Override
    public AttributeValue transformFrom(UserMetadata input) {
        try {
            return AttributeValue.builder().s(objectMapper.writeValueAsString(input)).build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public UserMetadata transformTo(AttributeValue attributeValue) {
        try {
            return objectMapper.readValue(attributeValue.s(), UserMetadata.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public EnhancedType<UserMetadata> type() {
        return EnhancedType.of(UserMetadata.class);
    }

    @Override
    public AttributeValueType attributeValueType() {
        return AttributeValueType.S;
    }
}
