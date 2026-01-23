package com.airplus.monitoring.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MonitoringKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringKafkaListener.class);

    @KafkaListener(topics = "${kafka.topic.monitoring}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        logger.info("Received message - Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }
}
