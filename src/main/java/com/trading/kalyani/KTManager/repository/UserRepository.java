package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.UserEntity;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends CrudRepository<UserEntity,String> {

    @Query(value="SELECT MAX(ID) FROM USERMODEL;",nativeQuery = true)
    public Long getLatestToken();

}
