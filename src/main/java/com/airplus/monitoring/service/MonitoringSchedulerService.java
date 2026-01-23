package com.airplus.monitoring.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Service
public class MonitoringSchedulerService {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringSchedulerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicName;

    private final Counter messagesSentCounter;
    private final Counter messagesAcknowledgedCounter;
    private final Counter messagesFailedCounter;
    private final Timer publishLatencyTimer;

    public MonitoringSchedulerService(KafkaTemplate<String, String> kafkaTemplate,
                                      @Value("${kafka.topic.monitoring}") String topicName,
                                      MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;

        this.messagesSentCounter = Counter.builder("kafka_messages_sent_total")
                .description("Total number of messages sent to Kafka")
                .tag("topic", topicName)
                .register(meterRegistry);

        this.messagesAcknowledgedCounter = Counter.builder("kafka_messages_acknowledged_total")
                .description("Total number of messages acknowledged by Kafka brokers")
                .tag("topic", topicName)
                .register(meterRegistry);

        this.messagesFailedCounter = Counter.builder("kafka_messages_failed_total")
                .description("Total number of messages that failed to send")
                .tag("topic", topicName)
                .register(meterRegistry);

        this.publishLatencyTimer = Timer.builder("kafka_publish_latency_seconds")
                .description("Time taken to publish message and receive broker acknowledgement")
                .tag("topic", topicName)
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(meterRegistry);
    }

    @Scheduled(fixedRate = 60000)
    public void sendMonitoringMessage() {
        String message = "Monitoring heartbeat at " + Instant.now();
        logger.info("Sending message to topic {}: {}", topicName, message);

        messagesSentCounter.increment();
        long startTime = System.nanoTime();

        kafkaTemplate.send(topicName, message)
                .whenComplete((result, exception) -> {
                    long duration = System.nanoTime() - startTime;
                    publishLatencyTimer.record(duration, TimeUnit.NANOSECONDS);

                    if (exception == null) {
                        messagesAcknowledgedCounter.increment();
                        logger.info("Message acknowledged - Topic: {}, Partition: {}, Offset: {}, Latency: {} ms",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                TimeUnit.NANOSECONDS.toMillis(duration));
                    } else {
                        messagesFailedCounter.increment();
                        logger.error("Failed to send message - Latency: {} ms, Error: {}",
                                TimeUnit.NANOSECONDS.toMillis(duration),
                                exception.getMessage());
                    }
                });
    }
}
