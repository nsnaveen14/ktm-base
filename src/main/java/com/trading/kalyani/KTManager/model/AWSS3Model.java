package com.trading.kalyani.KTManager.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AWSS3Model {

    private String bucketName;

    private String key;

    private String fileName;

    private boolean status;

    private String message;

}
