package com.tora.kafka.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageReceiver {

    public static final int POLL_TIMEOUT_MILLIS = 10_000;
    private final KafkaConsumer<String, String> consumer;

    public MessageReceiver(Properties properties) {
        consumer = new KafkaConsumer<>(properties);
    }

    public ConsumerRecords<String, String> receive() {
        return consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MILLIS));
    }

    public void subscribe(final List<String> topics) {
        consumer.subscribe(topics);
    }
}
