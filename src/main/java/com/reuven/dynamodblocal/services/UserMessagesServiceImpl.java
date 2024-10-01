package com.reuven.dynamodblocal.services;

import com.reuven.dynamodblocal.entities.UserMessages;
import com.reuven.dynamodblocal.repositories.UserMessagesRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

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


}
