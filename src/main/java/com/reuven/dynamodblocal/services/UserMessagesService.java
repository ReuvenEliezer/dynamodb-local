package com.reuven.dynamodblocal.services;

import com.reuven.dynamodblocal.entities.UserMessages;

public interface UserMessagesService {
    void save(UserMessages userMessages);
}
