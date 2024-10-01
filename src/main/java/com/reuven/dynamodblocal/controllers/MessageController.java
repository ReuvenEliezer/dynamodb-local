package com.reuven.dynamodblocal.controllers;

import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.repositories.UserMessagesRepository;
import com.reuven.dynamodblocal.services.UserMessagesService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class MessageController {

    private static final Logger logger = LogManager.getLogger(MessageController.class);

    private final UserMessagesService userMessagesService;

    public MessageController(UserMessagesService userMessagesService) {
        this.userMessagesService = userMessagesService;
    }

    @PostMapping("/messages")
    public void getMessages(@RequestBody UserMessages userMessages) {
        userMessagesService.save(userMessages);
    }

}
