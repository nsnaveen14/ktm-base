    package com.trading.kalyani.KTManager.controller;

    import com.trading.kalyani.KTManager.config.KiteConnectConfig;
    import com.trading.kalyani.KTManager.entity.DailyJobPlanner;
    import com.trading.kalyani.KTManager.model.CommonReqRes;
    import com.trading.kalyani.KTManager.model.Message;
    import com.trading.kalyani.KTManager.service.*;
    import com.trading.kalyani.KTManager.service.serviceImpl.JobServiceImpl;
    import com.zerodhatech.kiteconnect.KiteConnect;
    import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
    import com.zerodhatech.models.Instrument;
    import com.zerodhatech.models.User;
    import jakarta.servlet.http.HttpServletRequest;
    import jakarta.servlet.http.HttpServletResponse;
    import org.apache.logging.log4j.LogManager;
    import org.apache.logging.log4j.Logger;
    import org.jetbrains.annotations.NotNull;
    import org.json.JSONException;
    import org.springframework.beans.factory.annotation.Autowired;
    import org.springframework.beans.factory.annotation.Value;
    import org.springframework.http.HttpStatus;
    import org.springframework.http.ResponseEntity;
    import org.springframework.web.bind.annotation.CrossOrigin;
    import org.springframework.web.bind.annotation.GetMapping;
    import org.springframework.web.bind.annotation.PostMapping;
    import org.springframework.web.bind.annotation.RestController;

    import java.io.IOException;
    import java.util.List;

    import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;


    @RestController
    @CrossOrigin(value="*")
    public class LoginController {

        @Autowired
        UserService userService;

        @Autowired
        InstrumentService instrumentService;

        @Autowired
        KiteConnectConfig kiteConnectConfig;

        @Autowired
        AsyncService asyncService;

        @Autowired
        MessagingService messagingService;

        @Autowired
        DailyJobPlannerService dailyJobPlannerService;

        @Value("${kite.apiSecret}")
        private String apiSecret;

        @Value("${kite.webSocketUrl}")
        private String kiteWSBaseURL;

        private static final Logger logger = LogManager.getLogger(LoginController.class);

        private static String kiteWSURL;

        @GetMapping("/login")
        public void login(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
        {
            KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
            String url = kiteSdk.getLoginURL();
            logger.info("Login URL: {}", url);
            try {
                httpServletResponse.sendRedirect(url);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        @GetMapping("/zerodhaRedirect")
        public void zerodhaRedirect(@NotNull HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
            String requestToken;

            if(httpServletRequest.getParameter("status").equalsIgnoreCase("SUCCESS")) {
                System.out.println(httpServletRequest.getParameter("request_token"));
                requestToken = httpServletRequest.getParameter("request_token");
                KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
                try {
                    User userModel =  kiteSdk.generateSession(requestToken, apiSecret);
                    // Set request token and public token which are obtained from login process.
                    kiteSdk.setAccessToken(userModel.accessToken);
                    kiteSdk.setPublicToken(userModel.publicToken);
                    logger.info("Access Token: {}",userModel.accessToken);
                    logger.info("Public Token: {}",userModel.publicToken);
                    kiteWSURL = kiteWSBaseURL.replace("xxx",userModel.apiKey).replace("yyy",userModel.accessToken).trim() ;
                    logger.info("Web Socket URL: {}", kiteWSURL);
                    userModel.avatarURL=kiteWSURL;
                    List<Instrument> instruments = kiteConnectConfig.kiteConnect().getInstruments();
                //    List<Instrument> requiredInstruments = instrumentService.getRequiredInstruments(instruments);
                    asyncService.saveUserNInstrumentDataAsync(userModel,instruments)
                            .thenRun(() -> {
                                // Code to execute after successful completion
                                logger.info("Data saved successfully!");
                                CommonReqRes asyncmessage = new CommonReqRes();
                                asyncmessage.setMessage("User and Instrument data saved successfully!");
                                asyncmessage.setStatus(true);
                                asyncmessage.setType(SUCCESS);
                                messagingService.sendCommonMessage(asyncmessage);
                            }).thenRun(()-> {

                                // create an entry of daily job planner as per configurations
                                List<DailyJobPlanner> dailyJobPlannerEntry = dailyJobPlannerService.createDailyJobPlannerEntry();
                                logger.info("Daily Job Planner entry created successfully! {}", dailyJobPlannerEntry.toString());
                            })
                            .exceptionally(ex -> {
                                // Handle exceptions
                                logger.error("Error occurred: " + ex.getMessage());
                                CommonReqRes asyncmessage = new CommonReqRes();
                                asyncmessage.setMessage("Error during saving data: " + ex.getMessage());
                                asyncmessage.setStatus(false);
                                asyncmessage.setType(ERROR);
                                messagingService.sendCommonMessage(asyncmessage);
                                return null;
                            });

                    httpServletResponse.sendRedirect("/");
                } catch (KiteException | JSONException | IOException e) {
                    throw new RuntimeException(e);
                }

            }


            else httpServletResponse.sendRedirect("/error");;

        }


        @GetMapping("/error")
        public ResponseEntity<String> errorHandler() {
            return new ResponseEntity<>("ERROR DURING LOGIN PROCESS", HttpStatus.OK);
        }

        @GetMapping("getAccessTokenFromDB")
        public ResponseEntity<User> getAccessTokenFromDB()
        {

            KiteConnect kiteSdk = kiteConnectConfig.kiteConnect();
            User userModel = userService.getUserModel();
            if(userModel.userName!=null) {
                kiteSdk.setAccessToken(userModel.accessToken);
                kiteSdk.setPublicToken(userModel.publicToken);
            }

            return new ResponseEntity<User>(userModel, HttpStatus.OK);
        }

        @PostMapping("/createDailyJobPlanner")
        public ResponseEntity<List<DailyJobPlanner>> createDailyJobPlanner() {
            List<DailyJobPlanner> dailyJobPlanners = dailyJobPlannerService.createDailyJobPlannerEntry();
            if (dailyJobPlanners.isEmpty()) {
                return new ResponseEntity<>(dailyJobPlanners, HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(dailyJobPlanners, HttpStatus.OK);
        }



    }
