package com.trading.kalyani.KTManager.entity;

import com.zerodhatech.models.User;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name="Usermodel")
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
@Setter
public class UserEntity {

    @Id
    private String kiteUserName;

    @Embedded
    private User user;
}
