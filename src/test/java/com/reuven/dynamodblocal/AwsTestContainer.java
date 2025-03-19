package com.reuven.dynamodblocal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.TestcontainersExtension;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
@ExtendWith(TestcontainersExtension.class)
public abstract class AwsTestContainer {

    protected static final Logger logger = LogManager.getLogger(AwsTestContainer.class);
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:4.0.3");

    @Container
    protected LocalStackContainer localstack = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(getServices().toArray(new LocalStackContainer.Service[0]))
//            .withLogConsumer(outputFrame -> logger.info(outputFrame.getUtf8String()))
//            .withStartupTimeout(Duration.ofMinutes(1));
            ;

    @BeforeEach
    void setup() {
        if (!localstack.isRunning()) {
            localstack.start();
        }
    }


    @AfterEach
    void teardown() {
        if (localstack != null) {
            localstack.close();
        }
    }

    protected abstract List<LocalStackContainer.Service> getServices();

}
