package com.reuven.dynamodblocal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SqsSnsTest extends AwsTestContainer {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    private static SqsClient sqsClient;
    private static SnsClient snsClient;
    private static final int MAX_MESSAGES_PER_REQUEST = 10;

    @BeforeEach
    void setup() {
        super.setup();
        String accessKey = localstack.getAccessKey();
        String secretKey = localstack.getSecretKey();

        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey))
                .build();

        snsClient = SnsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SNS))
                .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey))
                .build();
    }


    @Test
    void sendMessageToSnsAndConsumeBySqsTest() throws JsonProcessingException {
        CreateTopicResponse createTopicResponse = createTopic("topicName");
        CreateQueueResponse createQueueResponse = createQueue("queueName");
        String queueUrl = createQueueResponse.queueUrl();
        String topicArn = createTopicResponse.topicArn();
        subscribeQueue(queueUrl, topicArn);
        String payload = "payload";
        snsClient.publish(PublishRequest.builder().topicArn(topicArn).message(payload).build());

        List<Message> messagesFromQueue = getMessagesFromQueue(queueUrl, Duration.ofSeconds(2));

        assertThat(messagesFromQueue).isNotEmpty();
        Message message = messagesFromQueue.getFirst();
        JsonNode jsonNode = objectMapper.readTree(message.body());
        String payloadFromMessage = jsonNode.get("Message").asText();
        assertThat(payloadFromMessage).isEqualTo(payload);
    }

    private static CreateQueueResponse createQueue(String queueName) {
        return sqsClient.createQueue(CreateQueueRequest.builder()
                .queueName(queueName)
                .build());
    }

    private static CreateTopicResponse createTopic(String topicName) {
        return snsClient.createTopic(CreateTopicRequest.builder()
                .name(topicName)
                .build());
    }

    @Test
    void sendMessageToSnsAndConsumeBy2DifferentSqsTest() throws JsonProcessingException {
        CreateTopicResponse createTopicResponse = createTopic("topicName");
        CreateQueueResponse createQueueResponse1 = createQueue("queueName1");
        CreateQueueResponse createQueueResponse2 = createQueue("queueName2");
        String topicArn = createTopicResponse.topicArn();

        subscribeQueue(createQueueResponse1.queueUrl(), topicArn);
        subscribeQueue(createQueueResponse2.queueUrl(), topicArn);
        String payload = "payload";
        snsClient.publish(PublishRequest.builder().topicArn(topicArn).message(payload).build());

        List<Message> messagesFromQueue1 = getMessagesFromQueue(createQueueResponse1.queueUrl(), Duration.ofSeconds(1));

        assertThat(messagesFromQueue1).isNotEmpty();
        Message message = messagesFromQueue1.getFirst();
        JsonNode jsonNode = objectMapper.readTree(message.body());
        String payloadFromMessage = jsonNode.get("Message").asText();
        assertThat(payloadFromMessage).isEqualTo(payload);

        List<Message> messagesFromQueue2 = getMessagesFromQueue(createQueueResponse2.queueUrl(), Duration.ofSeconds(2));

        assertThat(messagesFromQueue2).isNotEmpty();
        Message message2 = messagesFromQueue2.getFirst();
        JsonNode jsonNode2 = objectMapper.readTree(message2.body());
        String payloadFromMessage2 = jsonNode2.get("Message").asText();
        assertThat(payloadFromMessage2).isEqualTo(payload);

    }

    private void subscribeQueue(String queueUrl, String topicArn) {
        GetQueueAttributesResponse queueAttributes = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .queueUrl(queueUrl)
                .build());

        String queueArn = queueAttributes.attributes().get(QueueAttributeName.QUEUE_ARN);

        snsClient.subscribe(SubscribeRequest.builder()
                .topicArn(topicArn)
                .protocol("sqs")
                .endpoint(queueArn)
                .build());
    }

    private List<Message> getMessagesFromQueue(String queueUrl, Duration waitTimeDuration) {
        return sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(MAX_MESSAGES_PER_REQUEST)
                .waitTimeSeconds(Long.valueOf(waitTimeDuration.getSeconds()).intValue())
                .build()).messages();
    }

    @Override
    protected List<LocalStackContainer.Service> getServices() {
        return List.of(LocalStackContainer.Service.SNS, LocalStackContainer.Service.SQS);
    }
}
