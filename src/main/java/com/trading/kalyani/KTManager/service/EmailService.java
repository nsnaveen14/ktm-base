package com.trading.kalyani.KTManager.service;

import java.util.List;

public interface EmailService {
    boolean sendEmail(String to, String subject, String body);

    boolean sendEmailWithAttachment(String to, String subject, String body, String attachmentPath);

    int sendEmailToAllUsers(String subject, String body, String attachmentPath);

    boolean sendEmailWithHTMLBody(String to, String subject, String body);

    boolean sendEmailWithHTMLBodyAndAttachment(String to, String subject, String body, String attachmentPath);
}
