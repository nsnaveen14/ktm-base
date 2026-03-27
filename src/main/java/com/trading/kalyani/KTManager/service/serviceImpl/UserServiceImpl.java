package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.UserEntity;
import com.trading.kalyani.KTManager.model.UserModel;
import com.trading.kalyani.KTManager.repository.UserRepository;
import com.trading.kalyani.KTManager.service.UserService;
import com.zerodhatech.models.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class UserServiceImpl implements UserService {

    private static final Logger logger = LoggerFactory.getLogger(UserServiceImpl.class);

    @Autowired
    UserRepository userRepository;

    @Value("${kiteUserName}")
    String userName;

    public boolean saveUserModelData(User userModel)
    {

        try {
            Optional<UserEntity> existing = userRepository.findById(userName);
            UserEntity userEntity = existing.isPresent() ? existing.get() : new UserEntity(userName, new User());
            userEntity.setUser(userModel);
            userRepository.save(userEntity);
            return true;
        }
        catch(Exception ex)
        {
            logger.error("Error saving user model data for {}: {}", userName, ex.getMessage(), ex);
        }
        return false;
    }


    @Override
    public UserEntity getUserDetails(String userName) {
        return userRepository.findById(userName).orElseGet(UserEntity::new);
    }

    @Override
    public UserEntity addUser(UserModel userModel) {

        UserEntity userEntity = new UserEntity();
        userEntity.setKiteUserName(userModel.getUserName());
        userEntity.setUser(new User());

        userRepository.save(userEntity);

        return userEntity;
    }

    @Override
    public List<UserEntity> getUsers() {
        Iterable<UserEntity> allUsers = userRepository.findAll();
        List<UserEntity> listOfUsers = new ArrayList<>();
        allUsers.forEach(listOfUsers::add);
        return listOfUsers;
    }

    public User getUserModel() {

        User userModel;

        Optional<UserEntity> userEntityOpt = userRepository.findById(userName);
        if(userEntityOpt.isPresent())
        {
         userModel = userEntityOpt.get().getUser();

            LocalDateTime updatedTime = LocalDateTime.now().minusHours(24) ;
            LocalDateTime lastLogin = LocalDateTime.ofInstant(userModel.loginTime.toInstant(), ZoneId.systemDefault());

            if (lastLogin.isAfter(updatedTime))
                return userModel;
        }

        return new User();

    }

}
