package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.entity.AppJobConfig;
import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.JobIterationDetails;
import com.trading.kalyani.KTManager.entity.TradeDecisions;
import com.trading.kalyani.KTManager.service.AsyncService;
import com.trading.kalyani.KTManager.service.EmailManagement;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.EmailTemplates.*;
import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;


@Service
public class EmailManagementImpl implements EmailManagement {

    @Autowired
    AsyncService asyncService;

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    private static final Logger logger = LoggerFactory.getLogger(EmailManagementImpl.class);

    @Override
    public void sendEmailForErrorInJob(Exception e, AppJobConfig appJobConfig, Integer retryCounter, JobIterationDetails jobIterationDetails) {

        String delimiter = System.lineSeparator() ;
        String subject = "KTM: Error in Job Execution for " + appJobConfig.getAppJobConfigNum();
        String emailSubject = subject +
                SPACE +
                "from Profile: " +
                kiteConnectConfig.getActiveProfile();

        String emailBody = "KTM: An error occurred during the job execution for " + appJobConfig.getAppIndexConfig().getStrikePriceName() + delimiter +
                "Error Details: " + e.getMessage() + delimiter +
                "Job Iteration Details: " + jobIterationDetails.toString() + delimiter +
                "Remaining Retry Attempts: " + retryCounter + delimiter +
                "Please investigate the issue.";

        sendEmail(emailSubject, emailBody, "");

    }

    @Override
    public void sendEmailAsync(String fileName,AppJobConfig appJobConfig) {

        String emailBody = "OI Snapshot for: " +
                appJobConfig.getAppIndexConfig().getIndexName() +
                UNDER_SCORE +
                appJobConfig.getJobType().getJobType() +
                SPACE +
                "is taken successfully and attached with this email.";

        String emailSubject = "KTM: OISnapshot" +
                SPACE +
                "from Profile: " +
                kiteConnectConfig.getActiveProfile();

        asyncService.sendEmailToAllUsersAsync(emailSubject, emailBody, fileName)
                .thenRun(() -> {
                    // Code to execute after successful completion
                    logger.info("Email sent successfully!");
                    try {
                        File file = new File(fileName);
                        if (file.exists()) {
                            if (file.delete()) {
                                logger.info("Attachment file deleted successfully: {}", file.getAbsolutePath());
                            } else {
                                logger.warn("Failed to delete attachment file: {}", file.getAbsolutePath());
                            }
                        } else {
                            logger.warn("Attachment file not found for deletion: {}", file.getAbsolutePath());
                        }
                    } catch (Exception ex) {
                        logger.error("Error while deleting attachment file {}: {}", fileName, ex.getMessage());
                    }

                })
                .exceptionally(ex -> {
                    // Handle exceptions
                    logger.error("Error occurred: {}", ex.getMessage());
                    return null;
                });
    }

@Override
public void sendEmailOfAutoTradeDecision(IndexLTP indexLTP, AppJobConfig appJobConfig, TradeDecisions lastTradeDecisions) {

    String subject = "KTM: "+ lastTradeDecisions.getTradeDecisionType() + ": "+
            indexLTP.getTradeDecision()+
            " for "+
            appJobConfig.getAppIndexConfig().getIndexName()+
            "-"+
            appJobConfig.getJobType().getJobType() +
            " from Profile: "+ kiteConnectConfig.getActiveProfile();

 //   String emailBodyFromIndexLTP = getEmailBodyFromIndexLTP(indexLTP);
    String emailBodyFromIndexLTPFormatted = getEmailBodyFromIndexLTPFormatted(indexLTP);

 //   String emailBodyFromTradeDecision = getEmailBodyFromTradeDecision(lastTradeDecisions);
    String emailBodyFromTradeDecisionFormatted = getEmailBodyFromTradeDecisionFormatted(lastTradeDecisions, null);

    String emailBody = emailBodyFromIndexLTPFormatted + System.lineSeparator()  + emailBodyFromTradeDecisionFormatted;

    sendEmail(subject, emailBody, "");

}

    @NotNull
    private static String getEmailBodyFromIndexLTP(IndexLTP indexLTP) {
        String delimiter = System.lineSeparator() ;

        return "Trade Decision generated: "+
                indexLTP.getTradeDecision() + delimiter +
                "Index LTP: "+ indexLTP.getIndexLTP() + delimiter +
                "Support: "+ indexLTP.getSupport() + delimiter +
                "Resistance: "+ indexLTP.getResistance() + delimiter +
                "Trade Decision TS: "+ indexLTP.getIndexTS() + delimiter +
                "Range: "+ indexLTP.getRange() + delimiter +
                "Max Pain SP: "+ indexLTP.getMaxPain().getMaxPainSP() + delimiter +
                "Max Pain SP Second: "+ indexLTP.getMaxPain().getMaxPainSPSecond() + delimiter +
                "Max Pain Bias Ratio: "+String.format("%.2f", indexLTP.getMaxPain().getMaxPainBiasRatio()) + delimiter +
                "Day High: "+ indexLTP.getDayHigh() + delimiter +
                "Day Low: "+ indexLTP.getDayLow();
    }

    @NotNull
    private String getEmailBodyFromTradeDecision(TradeDecisions tradeDecisions) {
        String delimiter = System.lineSeparator() ;
        return "Trade Decision Details: "+ delimiter +
                "App Index Name: "+ tradeDecisions.getAppJobConfig().getAppIndexConfig().getIndexName() + delimiter +
                "Job Type: "+ tradeDecisions.getAppJobConfig().getJobType().getJobType() + delimiter +
                "Status: "+ tradeDecisions.getStatus() + delimiter +
                "Trade Decision: "+ tradeDecisions.getTradeDecision() + delimiter +
                "Index LTP: "+ tradeDecisions.getIndexLTP() + delimiter +
                "Entry Index LTP: "+ tradeDecisions.getEntryIndexLTP() + delimiter +
                "Target Index LTP: "+ tradeDecisions.getTargetIndexLTP() + delimiter +
                "Stop Loss: "+ tradeDecisions.getStopLossIndexLTP() + delimiter +
                "Trade Decision TS: "+ tradeDecisions.getTradeDecisionTS() + delimiter +
                "Swing Target: "+ tradeDecisions.getSwingTarget() + delimiter +
                "Is Swing Taken: "+ tradeDecisions.isSwingTaken() + delimiter +
                "Is Confirmation Taken: "+ tradeDecisions.isConfirmationTaken();

    }

    @Override
    public void sendEmailOfTradeDecision(IndexLTP indexLTP,AppJobConfig appJobConfig) {

        String subject = "KTM: Trade Decision: "+
                indexLTP.getTradeDecision()+
                " for "+
                appJobConfig.getAppIndexConfig().getIndexName()+
                "-"+
                appJobConfig.getJobType().getJobType() +
                " from Profile: "+ kiteConnectConfig.getActiveProfile();

     //   String emailBodyFromIndexLTP = getEmailBodyFromIndexLTP(indexLTP);
        String emailBodyFromIndexLTPFormatted = getEmailBodyFromIndexLTPFormatted(indexLTP);

        String emailBody = emailBodyFromIndexLTPFormatted + System.lineSeparator();

        sendEmail(subject, emailBody, "");




    }

    public void sendEmailOfPlacingAutoTradeOrder(TradeDecisions lastTradeDecisions,String instrumentToken) {

        String subject = "KTM: Auto Trade Order Placed: "+
                lastTradeDecisions.getTradeDecision()+
                " for "+
                lastTradeDecisions.getAppJobConfig().getAppIndexConfig().getIndexName()+
                "-"+
                lastTradeDecisions.getAppJobConfig().getJobType().getJobType() +
                " from Profile: "+ kiteConnectConfig.getActiveProfile();

        String emailBody = getEmailBodyFromTradeDecisionFormatted(lastTradeDecisions,instrumentToken);

        sendEmail(subject, emailBody, "");

    }

    private static String getEmailBodyFromTradeDecisionFormatted(TradeDecisions td, String instrumentToken) {

        String appIndexName = "N/A";
        String jobTypeName = "N/A";
        if (td != null && td.getAppJobConfig() != null && td.getAppJobConfig().getAppIndexConfig() != null) {
            appIndexName = safe(td.getAppJobConfig().getAppIndexConfig().getIndexName());
        }
        if (td != null && td.getAppJobConfig() != null && td.getAppJobConfig().getJobType() != null) {
            jobTypeName = safe(td.getAppJobConfig().getJobType().getJobType());
        }

        String status = td != null ? safe(td.getStatus()) : "N/A";
        String tradeDecision = td != null ? safe(td.getTradeDecision()) : "N/A";
        String indexLTP = td != null ? safe(td.getIndexLTP()) : "N/A";
        String entryIndex = td != null ? safe(td.getEntryIndexLTP()) : "N/A";
        String targetIndex = td != null ? safe(td.getTargetIndexLTP()) : "N/A";
        String stopLoss = td != null ? safe(td.getStopLossIndexLTP()) : "N/A";
        String decisionTs = td != null ? safe(td.getTradeDecisionTS()) : "N/A";
        String decisionResult = td != null ? safe(td.getTrade_decision_result()) : "N/A";
        String decisionResultTs = td != null ? safe(td.getTrade_decision_result_ts()) : "N/A";
        String jobIterationId = (td != null && td.getJobIterationDetails() != null) ? safe(td.getJobIterationDetails().getId()) : "N/A";
        String instrumentTokenStr = instrumentToken != null ? instrumentToken : "N/A";

        StringBuilder sb = new StringBuilder();
        sb.append(HTML_TEMPLATE_START);
        sb.append(TABLE_START);

        // header row
        sb.append(TABLE_ROW_START)
                .append(TABLE_HEADER_START)
                .append("Trade Decision Details")
                .append(TABLE_HEADER_END)
                .append(TABLE_ROW_END);

        // helper to append rows
        java.util.function.BiConsumer<String,String> row = (label, value) ->
                sb.append(TABLE_ROW_START)
                        .append(TABLE_DATA_LABEL_START)
                        .append(label).append(TABLE_DATA_END)
                        .append(TABLE_DATA_VALUE_START).append(value).append(TABLE_DATA_END)
                        .append(TABLE_ROW_END);

        row.accept("App / Index", appIndexName);
        row.accept("Job Type", jobTypeName);
        row.accept("Job Iteration Id", jobIterationId);
        row.accept("Status", status);
        row.accept("Trade Decision", tradeDecision);
        row.accept("Index LTP", indexLTP);
        row.accept("Entry Index LTP", entryIndex);
        row.accept("Target Index LTP", targetIndex);
        row.accept("Stop Loss Index LTP", stopLoss);
        row.accept("Trade Decision TS", decisionTs);
        row.accept("Trade Decision Result", decisionResult);
        row.accept("Trade Decision Result TS", decisionResultTs);
        row.accept("Instrument Token", instrumentTokenStr);

        sb.append(TABLE_END);
        sb.append(HTML_TEMPLATE_END);

        return sb.toString();
    }


    static String safe(Object o)
    {
        return o == null ? "N/A" : o.toString();
    }

    private static String getEmailBodyFromIndexLTPFormatted(IndexLTP indexLTP) {

        String biasRatio = "N/A";
        if (indexLTP != null && indexLTP.getMaxPain() != null) {
            double ratio = indexLTP.getMaxPain().getMaxPainBiasRatio();
            biasRatio = String.format("%.2f", ratio);
        }

        String tradeDecision = indexLTP != null ? safe(indexLTP.getTradeDecision()) : "N/A";

        StringBuilder sb = new StringBuilder();
        sb.append(HTML_TEMPLATE_START);
        sb.append(TABLE_START);

        // header row
        sb.append(TABLE_ROW_START)
                .append(TABLE_HEADER_START)
                .append("Index LTP Details")
                .append(TABLE_HEADER_END)
                .append(TABLE_ROW_END);

        // helper to append rows
        java.util.function.BiConsumer<String,String> row = (label, value) ->
                sb.append(TABLE_ROW_START)
                        .append(TABLE_DATA_LABEL_START)
                        .append(label).append(TABLE_DATA_END)
                        .append(TABLE_DATA_VALUE_START).append(value).append(TABLE_DATA_END)
                        .append(TABLE_ROW_END);

        row.accept("Trade Decision", tradeDecision);
        row.accept("Index LTP", indexLTP != null ? safe(indexLTP.getIndexLTP()) : "N/A");
        row.accept("Support", indexLTP != null ? safe(indexLTP.getSupport()) : "N/A");
        row.accept("Resistance", indexLTP != null ? safe(indexLTP.getResistance()) : "N/A");
        row.accept("Trade Decision TS", indexLTP != null ? safe(indexLTP.getIndexTS()) : "N/A");
        row.accept("Range", indexLTP != null ? safe(indexLTP.getRange()) : "N/A");

        row.accept("Max Pain SP", indexLTP != null && indexLTP.getMaxPain() != null ? safe(indexLTP.getMaxPain().getMaxPainSP()) : "N/A");
        row.accept("Max Pain SP Second", indexLTP != null && indexLTP.getMaxPain() != null ? safe(indexLTP.getMaxPain().getMaxPainSPSecond()) : "N/A");
        row.accept("Max Pain Bias Ratio", biasRatio);

        row.accept("Day High", indexLTP != null ? safe(indexLTP.getDayHigh()) : "N/A");
        row.accept("Day Low", indexLTP != null ? safe(indexLTP.getDayLow()) : "N/A");

        sb.append(TABLE_END);
        sb.append(HTML_TEMPLATE_END);

        return sb.toString();
    }

    @Override
    public void sendEmailForClosingTrade(IndexLTP indexLTP, AppJobConfig appJobConfig, TradeDecisions tradeDecisions) {

        String subject = "KTM: Close Trade: "+
                tradeDecisions.getTradeDecision()+
                " for "+
                appJobConfig.getAppIndexConfig().getIndexName()+
                "-"+
                appJobConfig.getJobType().getJobType() +
                " from Profile: "+ kiteConnectConfig.getActiveProfile();

    //    String emailBody = getEmailBodyFromIndexLTP(indexLTP);
        String emailBodyFormatted = getEmailBodyFromIndexLTPFormatted(indexLTP);

        sendEmail(subject, emailBodyFormatted, "");

    }

    @Override
    public void sendEmailForJobStopped(Integer appJobConfigNum) {

        String subject = "KTM: Job Stopped for AppJobConfigNum: "+
                appJobConfigNum +
                " from Profile: "+ kiteConnectConfig.getActiveProfile();

        String emailBody = "The job for AppJobConfigNum: "+
                appJobConfigNum +
                " has been stopped successfully.";

        sendEmail(subject, emailBody, "");

    }

    @Override
    public void sendEmailForRangeChange(IndexLTP indexLTP, IndexLTP prevItrIndexLTP, AppJobConfig appJobConfig) {

        String subject = "KTM: Range Change Detected for "+
                appJobConfig.getAppIndexConfig().getIndexName()+
                "-"+
                appJobConfig.getJobType().getJobType() +
                " from Profile: "+ kiteConnectConfig.getActiveProfile();

        String emailBodyCurrentIndexLTP = getEmailBodyFromIndexLTPFormatted(indexLTP);

        String emailBodyPrevIndexLTP = getEmailBodyFromIndexLTPFormatted(prevItrIndexLTP);

        String emailBody = "Range change detected for "+
                appJobConfig.getAppIndexConfig().getIndexName()+
                "-"+
                appJobConfig.getJobType().getJobType() + System.lineSeparator() +
                "Previous Range: "  + System.lineSeparator() + emailBodyPrevIndexLTP + System.lineSeparator() +
                "Current Range: " + System.lineSeparator() + emailBodyCurrentIndexLTP;

        sendEmail(subject, emailBody, "");
    }

    private void sendEmail(String subject, String emailBody, String attachmentPath) {

        asyncService.sendEmailToAllUsersAsync(subject, emailBody, attachmentPath)
                .thenRun(() -> {
                    // Code to execute after successful completion
                    logger.info("Email sent successfully! for subject: {}", subject);
                })
                .exceptionally(ex -> {
                    // Handle exceptions
                    logger.error("Error occurred: {}", ex.getMessage());
                    return null;
                });

    }

}
