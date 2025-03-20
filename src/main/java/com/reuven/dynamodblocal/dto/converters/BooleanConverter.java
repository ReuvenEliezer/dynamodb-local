//package com.reuven.dynamodblocal.dto.converters;
//
//import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
//import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
//import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
//import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
//
//public class BooleanConverter implements AttributeConverter<Boolean> {
//
//    private static final String TRUE_VALUE = "1";
//
//    @Override
//    public AttributeValue transformFrom(Boolean input) {
//        return AttributeValue.fromBool(input);
//    }
//
//    @Override
//    public Boolean transformTo(AttributeValue input) {
//        return TRUE_VALUE.equals(input.n());
//    }
//
//    @Override
//    public EnhancedType<Boolean> type() {
//        return EnhancedType.of(Boolean.class);
//    }
//
//    @Override
//    public AttributeValueType attributeValueType() {
//        return AttributeValueType.N;
//    }
//
//}
