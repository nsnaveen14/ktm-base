package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.UserEntity;
import com.trading.kalyani.KTManager.model.UserModel;
import com.zerodhatech.models.User;

import java.util.List;


public interface UserService {
    boolean saveUserModelData( User userModel);

    UserEntity getUserDetails(String userName);

    UserEntity addUser(UserModel userModel);

    List<UserEntity> getUsers();

    User getUserModel();
}
