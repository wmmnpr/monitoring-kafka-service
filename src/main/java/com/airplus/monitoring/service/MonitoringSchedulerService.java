package com.airplus.monitoring.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class MonitoringSchedulerService {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringSchedulerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MessageTracker messageTracker;
    private final String topicName;
    private final double publishLatencyThresholdSeconds;
    private final WebClient webClient;
    private final long alertCooldownMs;
    private Instant lastAlertSentAt = Instant.MIN;

    private final Counter messagesSentCounter;
    private final Counter messagesAcknowledgedCounter;
    private final Counter messagesFailedCounter;
    private final Timer publishLatencyTimer;

    public MonitoringSchedulerService(KafkaTemplate<String, String> kafkaTemplate,
                                      MessageTracker messageTracker,
                                      @Value("${kafka.topic.monitoring}") String topicName,
                                      @Value("${kafka.monitoring.publish-latency-threshold-seconds}") double publishLatencyThresholdSeconds,
                                      @Value("${kafka.monitoring.alert.endpoint}") String alertEndpoint,
                                      @Value("${kafka.monitoring.alert.username}") String alertUsername,
                                      @Value("${kafka.monitoring.alert.password}") String alertPassword,
                                      @Value("${kafka.monitoring.alert.cooldown-seconds}") long alertCooldownSeconds,
                                      MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageTracker = messageTracker;
        this.topicName = topicName;
        this.publishLatencyThresholdSeconds = publishLatencyThresholdSeconds;
        this.alertCooldownMs = TimeUnit.SECONDS.toMillis(alertCooldownSeconds);
        this.webClient = WebClient.builder()
                .baseUrl(alertEndpoint)
                .defaultHeaders(headers -> headers.setBasicAuth(alertUsername, alertPassword))
                .build();

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
        String messageId = UUID.randomUUID().toString();
        String message = String.format("{\"id\":\"%s\",\"timestamp\":\"%s\"}", messageId, Instant.now());

        messageTracker.trackMessage(messageId);
        logger.info("Sending message to topic {} with ID {}", topicName, messageId);

        messagesSentCounter.increment();
        long startTime = System.nanoTime();

        kafkaTemplate.send(topicName, messageId, message)
                .whenComplete((result, exception) -> {
                    long duration = System.nanoTime() - startTime;
                    publishLatencyTimer.record(duration, TimeUnit.NANOSECONDS);

                    if (exception == null) {
                        messagesAcknowledgedCounter.increment();
                        logger.info("Message acknowledged - ID: {}, Partition: {}, Offset: {}, Latency: {} ms",
                                messageId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                TimeUnit.NANOSECONDS.toMillis(duration));
                    } else {
                        messagesFailedCounter.increment();
                        logger.error("Failed to send message - ID: {}, Latency: {} ms, Error: {}",
                                messageId,
                                TimeUnit.NANOSECONDS.toMillis(duration),
                                exception.getMessage());
                    }
                });
    }

    @Scheduled(fixedRate = 30000)
    public void checkPublishLatency() {
        HistogramSnapshot snapshot = publishLatencyTimer.takeSnapshot();
        for (var pv : snapshot.percentileValues()) {
            if (pv.percentile() == 0.95) {
                double p95Seconds = pv.value(TimeUnit.SECONDS);
                if (p95Seconds > publishLatencyThresholdSeconds) {
                    logger.warn("p95 publish latency {}s exceeds threshold {}s for topic {}",
                            String.format("%.3f", p95Seconds), String.format("%.3f", publishLatencyThresholdSeconds), topicName);
                    if (Duration.between(lastAlertSentAt, Instant.now()).toMillis() >= alertCooldownMs) {
                        sendLatencyAlert(p95Seconds);
                    } else {
                        logger.debug("Latency alert suppressed, cooldown active until {}",
                                lastAlertSentAt.plusMillis(alertCooldownMs));
                    }
                } else {
                    logger.debug("p95 publish latency {}s within threshold {}s for topic {}",
                            String.format("%.3f", p95Seconds), String.format("%.3f", publishLatencyThresholdSeconds), topicName);
                }
                return;
            }
        }
        logger.debug("No p95 percentile data available yet for topic {}", topicName);
    }

    private void sendLatencyAlert(double p95Seconds) {
        lastAlertSentAt = Instant.now();

        String payload = String.format(
                "{\"topic\":\"%s\",\"p95LatencySeconds\":%.3f,\"thresholdSeconds\":%.3f,\"timestamp\":\"%s\"}",
                topicName, p95Seconds, publishLatencyThresholdSeconds, lastAlertSentAt);

        webClient.post()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .retrieve()
                .toBodilessEntity()
                .doOnSuccess(response -> logger.info("Latency alert sent successfully"))
                .doOnError(e -> logger.error("Failed to send latency alert: {}", e.getMessage()))
                .subscribe();
    }
}
