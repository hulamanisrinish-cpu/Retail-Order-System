package com.retailordersystem;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

@SpringBootApplication(exclude = KafkaAutoConfiguration.class)
public class RetailOrderSystemApplication {

    public static void main(String[] args) {
        SpringApplication.run(RetailOrderSystemApplication.class, args);
    }

}
