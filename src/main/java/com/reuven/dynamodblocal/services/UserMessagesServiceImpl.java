package com.reuven.dynamodblocal.services;

import com.reuven.dynamodblocal.dto.UserMessagesPageResponse;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse1;
import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.repositories.UserMessagesRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class UserMessagesServiceImpl implements UserMessagesService {

    private static final Logger logger = LogManager.getLogger(UserMessagesServiceImpl.class);

    private final UserMessagesRepository userMessagesRepository;

    public UserMessagesServiceImpl(UserMessagesRepository userMessagesRepository) {
        this.userMessagesRepository = userMessagesRepository;
    }

    @Override
    public void save(UserMessages userMessages) {
        userMessagesRepository.save(userMessages);
    }

    @Override
    public List<UserMessages> getUserMessages(String userId) {
        return userMessagesRepository.getUserMessages(userId);
    }

    @Override
    public List<UserMessages> getUserMessages(String userId, int limit) {
        return userMessagesRepository.getUserMessages(userId, limit);
    }

    @Override
    public UserMessagesPageResponse1 getUserMessagesPage(String userId, int limit, String page) {
        return userMessagesRepository.getUserMessagesPage(userId, limit, page);
    }

}
