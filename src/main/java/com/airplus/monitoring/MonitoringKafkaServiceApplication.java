package com.airplus.monitoring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MonitoringKafkaServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MonitoringKafkaServiceApplication.class, args);
    }
}
