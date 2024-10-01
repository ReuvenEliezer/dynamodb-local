package com.reuven.dynamodblocal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = {
        "com.reuven.dynamodblocal.config",
        "com.reuven.dynamodblocal.controllers",
        "com.reuven.dynamodblocal.services",
        "com.reuven.dynamodblocal.repositories"
})
@SpringBootApplication
public class DynamodbLocalApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamodbLocalApplication.class, args);
    }

}
