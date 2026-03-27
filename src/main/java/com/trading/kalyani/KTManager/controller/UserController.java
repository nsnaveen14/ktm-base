package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.UserEntity;
import com.trading.kalyani.KTManager.model.UserModel;
import com.trading.kalyani.KTManager.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin(value="*")
public class UserController {

    @Autowired
    UserService userService;

    @GetMapping("/getUserDetails")
    public ResponseEntity<UserEntity> getUserDetails(@RequestParam("userName") String userName) {
         return new ResponseEntity<>(userService.getUserDetails(userName), HttpStatus.OK);
    }

    @PostMapping("/addUser")
    public ResponseEntity<UserEntity> addUser(@RequestBody UserModel userModel) {
        return new ResponseEntity<>(userService.addUser(userModel), HttpStatus.OK);
    }

    @GetMapping("/getUsers")
    public ResponseEntity<List<UserEntity>> getUsers() {
        return new ResponseEntity<>(userService.getUsers(),HttpStatus.OK);
    }


}
