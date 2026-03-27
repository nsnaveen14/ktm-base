package com.trading.kalyani.KTManager.config;


import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenAPIConfig {

    @Bean
    public OpenAPI ktManagerOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("KTManager API")
                        .version("v1")
                        .description("APIs for KTManager project")
                        .contact(new Contact().name("KTManager").email("navisa07@gmail.com"))
                        .license(new License().name("MIT")));
    }
}
