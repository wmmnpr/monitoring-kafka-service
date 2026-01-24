package com.airplus.monitoring.listener;

import com.airplus.monitoring.service.MessageTracker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
public class MonitoringKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringKafkaListener.class);

    private final MessageTracker messageTracker;
    private final Timer roundTripTimer;

    public MonitoringKafkaListener(MessageTracker messageTracker,
                                   MeterRegistry meterRegistry,
                                   @Value("${kafka.topic.monitoring}") String topicName) {
        this.messageTracker = messageTracker;
        this.roundTripTimer = Timer.builder("kafka_message_round_trip_seconds")
                .description("End-to-end round trip time from send to consume")
                .tag("topic", topicName)
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(meterRegistry);
    }

    @KafkaListener(topics = "${kafka.topic.monitoring}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        String messageId = record.key();

        logger.info("Received message - Topic: {}, Partition: {}, Offset: {}, ID: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                messageId);

        if (messageId != null) {
            Optional<Long> roundTripMs = messageTracker.acknowledgeMessage(messageId);
            roundTripMs.ifPresent(duration -> {
                roundTripTimer.record(duration, TimeUnit.MILLISECONDS);
                logger.info("Message round-trip completed - ID: {}, Duration: {} ms", messageId, duration);
            });
        }
    }
}
