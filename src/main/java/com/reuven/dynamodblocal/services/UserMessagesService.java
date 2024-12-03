package com.reuven.dynamodblocal.services;

import com.reuven.dynamodblocal.dto.UserMessagesPageResponse;
import com.reuven.dynamodblocal.dto.UserMessagesPageResponse1;
import com.reuven.dynamodblocal.entities.UserMessages;

import java.util.List;

public interface UserMessagesService {

    void save(UserMessages userMessages);

    List<UserMessages> getUserMessages(String userId);

    List<UserMessages> getUserMessages(String userId, int limit);

    UserMessagesPageResponse1 getUserMessagesPage(String userId, int limit, String page);

}
