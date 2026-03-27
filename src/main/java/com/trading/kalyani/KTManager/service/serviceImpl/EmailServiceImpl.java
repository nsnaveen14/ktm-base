

package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.EmailConfig;
import com.trading.kalyani.KTManager.service.EmailService;
import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;


@Service
public class EmailServiceImpl implements EmailService {

    @Autowired
    EmailConfig emailConfig;

    private static final Logger logger = LogManager.getLogger(EmailServiceImpl.class);

    public boolean sendEmail(String to, String subject, String body) {

        boolean isMailSent = false;

        try {
            Message message = new MimeMessage(emailConfig.getEmailSession());
            message.setFrom(new InternetAddress(emailConfig.getUsername()));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(subject);
            message.setText(body);

            Transport.send(message);
            isMailSent = true;
        } catch (MessagingException e) {
            logger.error("Failed to send email to {}: {}", to, e.getMessage());

        }
        return isMailSent;
    }

    @Override
    public boolean sendEmailWithAttachment(String to, String subject, String body, String attachmentPath) {
        boolean isMailSent = false;

        try {
            Message message = new MimeMessage(emailConfig.getEmailSession());
            message.setFrom(new InternetAddress(emailConfig.getUsername()));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(subject);

            // Create the message body part
            MimeBodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setText(body);

            // Create the attachment part
            MimeBodyPart attachmentPart = new MimeBodyPart();
            attachmentPart.attachFile(attachmentPath);

            // Combine the parts into a multipart
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);
            multipart.addBodyPart(attachmentPart);

            // Set the content of the message
            message.setContent(multipart);

            // Send the email
            Transport.send(message);
            isMailSent = true;
        } catch (Exception e) {
            logger.error("Failed to send email with attachment to {}: {}", to, e.getMessage());
        }

        return isMailSent;
    }

    @Override
    public int sendEmailToAllUsers( String subject, String body, String attachmentPath) {

        List<String> emailIds = emailConfig.getUserListForEmail();

        int successCount = 0;
        for (String emailId : emailIds) {
            boolean isMailSent = false;
            if(attachmentPath.isBlank()) {
                if(body.contains("<html>"))
                    isMailSent = sendEmailWithHTMLBody(emailId, subject, body);
                else
                    isMailSent = sendEmail(emailId, subject, body);

            }
            else {
                if(body.contains("<html>"))
                    isMailSent = sendEmailWithHTMLBodyAndAttachment(emailId, subject, body, attachmentPath);
                else
                    isMailSent = sendEmailWithAttachment(emailId, subject, body, attachmentPath);
            }

            if (isMailSent) {
                successCount++;
            }
        }
        return successCount;
    }

    @Override
    public boolean sendEmailWithHTMLBody(String to, String subject, String body) {

        boolean isMailSent = false;

        try {
            Message message = new MimeMessage(emailConfig.getEmailSession());
            message.setFrom(new InternetAddress(emailConfig.getUsername()));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(subject);

            // Render HTML body
            message.setContent(body, "text/html; charset=utf-8");

            Transport.send(message);
            isMailSent = true;
        } catch (MessagingException e) {
            logger.error("Failed to send email to {}: {}", to, e.getMessage());
        }
        return isMailSent;
    }

    @Override
    public boolean sendEmailWithHTMLBodyAndAttachment(String to, String subject, String body, String attachmentPath) {
        boolean isMailSent = false;

        try {
            Message message = new MimeMessage(emailConfig.getEmailSession());
            message.setFrom(new InternetAddress(emailConfig.getUsername()));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(subject);

            // Create the message body part and set HTML content
            MimeBodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setContent(body, "text/html; charset=utf-8");

            // Create the attachment part
            MimeBodyPart attachmentPart = new MimeBodyPart();
            attachmentPart.attachFile(attachmentPath);

            // Combine the parts into a multipart
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);
            multipart.addBodyPart(attachmentPart);

            // Set the content of the message
            message.setContent(multipart);

            // Send the email
            Transport.send(message);
            isMailSent = true;
        } catch (Exception e) {
            logger.error("Failed to send email with attachment to {}: {}", to, e.getMessage());
        }

        return isMailSent;
    }



}