package com.airplus.monitoring.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MessageTracker {

    private static final Logger logger = LoggerFactory.getLogger(MessageTracker.class);

    private final Map<String, Instant> pendingMessages = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;
    private final long maxAgeMs;

    private final AtomicLong oldestMessageAgeMs = new AtomicLong(0);
    private final AtomicLong pendingMessageCount = new AtomicLong(0);

    private Counter trackedCounter;
    private Counter acknowledgedCounter;
    private Counter evictedCounter;

    public MessageTracker(MeterRegistry meterRegistry,
                          @Value("${kafka.tracker.max-age-ms:300000}") long maxAgeMs) {
        this.meterRegistry = meterRegistry;
        this.maxAgeMs = maxAgeMs;
    }

    @PostConstruct
    public void registerMetrics() {
        Gauge.builder("kafka_message_oldest_pending_age_ms", oldestMessageAgeMs, AtomicLong::get)
                .description("Age in milliseconds of the oldest pending message awaiting acknowledgement")
                .register(meterRegistry);

        Gauge.builder("kafka_message_pending_count", pendingMessageCount, AtomicLong::get)
                .description("Number of messages pending acknowledgement")
                .register(meterRegistry);

        trackedCounter = Counter.builder("kafka_message_tracked_total")
                .description("Total number of messages added to the tracker")
                .register(meterRegistry);

        acknowledgedCounter = Counter.builder("kafka_message_consumer_acknowledged_total")
                .description("Total number of messages acknowledged by the consumer")
                .register(meterRegistry);

        evictedCounter = Counter.builder("kafka_message_evicted_total")
                .description("Total number of messages evicted due to timeout (potential message loss)")
                .register(meterRegistry);

        logger.info("MessageTracker metrics registered with Prometheus (maxAgeMs: {})", maxAgeMs);
    }

    public void trackMessage(String messageId) {
        pendingMessages.put(messageId, Instant.now());
        pendingMessageCount.set(pendingMessages.size());
        trackedCounter.increment();
        logger.debug("Tracking message: {}", messageId);
    }

    public Optional<Long> acknowledgeMessage(String messageId) {
        Instant sentTime = pendingMessages.remove(messageId);
        pendingMessageCount.set(pendingMessages.size());

        if (sentTime != null) {
            acknowledgedCounter.increment();
            long roundTripMs = Instant.now().toEpochMilli() - sentTime.toEpochMilli();
            logger.debug("Message acknowledged: {}, round-trip: {} ms", messageId, roundTripMs);
            return Optional.of(roundTripMs);
        }

        logger.warn("Received unknown message ID: {}", messageId);
        return Optional.empty();
    }

    @Scheduled(fixedRateString = "${kafka.tracker.eviction-interval-ms:10000}")
    public void evictExpiredMessages() {
        if (pendingMessages.isEmpty()) {
            oldestMessageAgeMs.set(0);
            return;
        }

        Instant now = Instant.now();
        long cutoffTime = now.toEpochMilli() - maxAgeMs;
        List<String> expiredIds = new ArrayList<>();
        long maxAge = 0;

        for (Map.Entry<String, Instant> entry : pendingMessages.entrySet()) {
            long sentTimeMs = entry.getValue().toEpochMilli();
            long age = now.toEpochMilli() - sentTimeMs;

            if (sentTimeMs < cutoffTime) {
                expiredIds.add(entry.getKey());
                logger.warn("Evicting expired message - ID: {}, Age: {} ms", entry.getKey(), age);
            } else {
                maxAge = Math.max(maxAge, age);
            }
        }

        for (String expiredId : expiredIds) {
            pendingMessages.remove(expiredId);
            evictedCounter.increment();
        }

        if (!expiredIds.isEmpty()) {
            pendingMessageCount.set(pendingMessages.size());
            logger.warn("Evicted {} expired messages (max age: {} ms)", expiredIds.size(), maxAgeMs);
        }

        oldestMessageAgeMs.set(maxAge);

        if (maxAge > 0) {
            logger.info("Oldest pending message age: {} ms, pending count: {}", maxAge, pendingMessages.size());
        }
    }

    public int getPendingCount() {
        return pendingMessages.size();
    }
}
