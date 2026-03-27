package com.trading.kalyani.KTManager.config;

import jakarta.mail.Authenticator;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Configuration
@Getter
@Setter
public class EmailConfig {

    private static final Logger logger = LoggerFactory.getLogger(EmailConfig.class);

    @Value("${email.smtp.host}")
    private String smtpHost;

    @Value("${email.smtp.port}")
    private int smtpPort;

    @Value("${email.username}")
    private String username;

    @Value("${email.password}")
    private String password;

    @Bean
    public List<String> getUserListForEmail() {
        String csvFile = "userEmailIds.csv"; // File path relative to the resources folder
        String line;
        String delimiter = ","; // Adjust the delimiter if necessary (e.g., ";" or "\t")

        List<String> emailIds = new ArrayList<>();

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(csvFile)) {
            if (inputStream == null) {
                logger.error("userEmailIds.csv not found in classpath");
                return emailIds;
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {

                while ((line = br.readLine()) != null) {
                    // Split the line into columns
                    emailIds.addAll(Arrays.stream(line.split(delimiter)).map(String::trim).toList());
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load email list from CSV: {}", e.getMessage(), e);
        }

        return emailIds;
    }

    @Bean
    public Session getEmailSession() {

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.port", smtpPort);

        return Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

    }

}