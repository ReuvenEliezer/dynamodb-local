package com.reuven.dynamodblocal.controllers;

import com.reuven.dynamodblocal.dto.UserMessagesPageResponse;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse1;
import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.services.UserMessagesService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/")
public class MessageController {

    private static final Logger logger = LogManager.getLogger(MessageController.class);
    private static final String SIZE = "size";
    private static final String PAGE = "page";
    private static final String DEFAULT_MESSAGES_PAGE_SIZE = "10";

    private final UserMessagesService userMessagesService;

    public MessageController(UserMessagesService userMessagesService) {
        this.userMessagesService = userMessagesService;
    }

    @PostMapping("/message")
    public void sendMessage(@RequestBody UserMessages userMessages) {
        userMessagesService.save(userMessages);
    }

    @GetMapping("/get-messages")
    public List<UserMessages> getUserMessages(@RequestParam("user-id") String userId,
                                              @RequestParam(defaultValue = DEFAULT_MESSAGES_PAGE_SIZE, value = SIZE) int size) {
        return userMessagesService.getUserMessages(userId, size);
    }

    @GetMapping("/get-messages-page")
    public UserMessagesPageResponse1 getUserMessagesPage(@RequestParam("user-id") String userId,
                                                         @RequestParam(defaultValue = DEFAULT_MESSAGES_PAGE_SIZE, value = SIZE) int size,
                                                         @RequestParam(required = false, value = PAGE) String page) {
        logger.info("Getting messages with size: {} and page: {}", size, page);
        return userMessagesService.getUserMessagesPage(userId, size, page);
    }

}
