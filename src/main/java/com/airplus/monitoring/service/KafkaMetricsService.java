package com.airplus.monitoring.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaMetricsService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsService.class);

    private final KafkaAdmin kafkaAdmin;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final String topicName;
    private final String consumerGroupId;

    private final Map<TopicPartition, AtomicLong> partitionLagMap = new ConcurrentHashMap<>();
    private final AtomicLong totalLag = new AtomicLong(0);
    private final AtomicLong recordQueueTimeAvg = new AtomicLong(0);
    private final AtomicLong bufferAvailableBytes = new AtomicLong(0);
    private final AtomicLong batchSizeAvg = new AtomicLong(0);
    private final AtomicLong recordsPerRequestAvg = new AtomicLong(0);

    public KafkaMetricsService(KafkaAdmin kafkaAdmin,
                               KafkaTemplate<String, String> kafkaTemplate,
                               MeterRegistry meterRegistry,
                               @Value("${kafka.topic.monitoring}") String topicName,
                               @Value("${spring.kafka.consumer.group-id}") String consumerGroupId) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.topicName = topicName;
        this.consumerGroupId = consumerGroupId;
    }

    @PostConstruct
    public void registerMetrics() {
        // Consumer lag metrics
        Gauge.builder("kafka_consumer_lag_total", totalLag, AtomicLong::get)
                .description("Total consumer lag across all partitions")
                .tag("topic", topicName)
                .tag("consumer_group", consumerGroupId)
                .register(meterRegistry);

        // Producer queue metrics
        Gauge.builder("kafka_producer_record_queue_time_avg_ms", recordQueueTimeAvg, AtomicLong::get)
                .description("Average time in ms a record spends in the producer queue")
                .register(meterRegistry);

        Gauge.builder("kafka_producer_buffer_available_bytes", bufferAvailableBytes, AtomicLong::get)
                .description("Available bytes in the producer buffer")
                .register(meterRegistry);

        Gauge.builder("kafka_producer_batch_size_avg", batchSizeAvg, AtomicLong::get)
                .description("Average batch size in bytes")
                .register(meterRegistry);

        Gauge.builder("kafka_producer_records_per_request_avg", recordsPerRequestAvg, AtomicLong::get)
                .description("Average number of records per request")
                .register(meterRegistry);

        logger.info("Kafka metrics registered with Prometheus");
    }

    @Scheduled(fixedRate = 30000)
    public void collectMetrics() {
        collectConsumerLagMetrics();
        collectProducerMetrics();
    }

    private void collectConsumerLagMetrics() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Get consumer group offsets
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(consumerGroupId);
            Map<TopicPartition, OffsetAndMetadata> consumerOffsets = offsetsResult.partitionsToOffsetAndMetadata().get();

            if (consumerOffsets.isEmpty()) {
                logger.debug("No consumer offsets found for group: {}", consumerGroupId);
                return;
            }

            // Get end offsets for the partitions
            Map<TopicPartition, OffsetSpec> offsetSpecMap = new HashMap<>();
            for (TopicPartition tp : consumerOffsets.keySet()) {
                if (tp.topic().equals(topicName)) {
                    offsetSpecMap.put(tp, OffsetSpec.latest());
                }
            }

            if (offsetSpecMap.isEmpty()) {
                logger.debug("No partitions found for topic: {}", topicName);
                return;
            }

            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            adminClient.listOffsets(offsetSpecMap).all().get().forEach((tp, offsetInfo) ->
                    endOffsets.put(tp, offsetInfo.offset()));

            // Calculate lag per partition
            long totalLagValue = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumerOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                if (!tp.topic().equals(topicName)) {
                    continue;
                }

                long consumerOffset = entry.getValue().offset();
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long lag = Math.max(0, endOffset - consumerOffset);

                // Register per-partition gauge if not exists
                partitionLagMap.computeIfAbsent(tp, partition -> {
                    AtomicLong partitionLag = new AtomicLong(0);
                    Gauge.builder("kafka_consumer_lag_partition", partitionLag, AtomicLong::get)
                            .description("Consumer lag for specific partition")
                            .tag("topic", tp.topic())
                            .tag("partition", String.valueOf(tp.partition()))
                            .tag("consumer_group", consumerGroupId)
                            .register(meterRegistry);
                    return partitionLag;
                }).set(lag);

                totalLagValue += lag;

                logger.debug("Partition {} lag: {} (consumer: {}, end: {})",
                        tp.partition(), lag, consumerOffset, endOffset);
            }

            totalLag.set(totalLagValue);
            logger.info("Total consumer lag for topic {}: {}", topicName, totalLagValue);

        } catch (Exception e) {
            logger.error("Failed to collect consumer lag metrics: {}", e.getMessage());
        }
    }

    private void collectProducerMetrics() {
        try {
            Map<MetricName, ? extends Metric> metrics = kafkaTemplate.metrics();

            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                MetricName metricName = entry.getKey();
                Object value = entry.getValue().metricValue();

                if (!(value instanceof Number)) {
                    continue;
                }

                double numericValue = ((Number) value).doubleValue();

                switch (metricName.name()) {
                    case "record-queue-time-avg":
                        recordQueueTimeAvg.set((long) numericValue);
                        logger.debug("Producer record-queue-time-avg: {} ms", numericValue);
                        break;
                    case "buffer-available-bytes":
                        bufferAvailableBytes.set((long) numericValue);
                        logger.debug("Producer buffer-available-bytes: {}", numericValue);
                        break;
                    case "batch-size-avg":
                        batchSizeAvg.set((long) numericValue);
                        logger.debug("Producer batch-size-avg: {} bytes", numericValue);
                        break;
                    case "records-per-request-avg":
                        recordsPerRequestAvg.set((long) numericValue);
                        logger.debug("Producer records-per-request-avg: {}", numericValue);
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to collect producer metrics: {}", e.getMessage());
        }
    }
}
