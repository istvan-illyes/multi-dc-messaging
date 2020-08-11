package com.tora.kafka.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ProducerMain {

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("enable.idempotence", "true");
        properties.put("batch.size", "65536"); // 64KB
        properties.put("buffer.memory", "268435456"); // 256MB
        properties.put("linger.ms", "10"); // wait up to this number of milliseconds before sending a request in hope that more records will arrive to fill up the same batch
        properties.put("compression.type", "lz4");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final MessageSender messageSender = new MessageSender(properties);
        final List<String> topics = Arrays.asList("test-topic-1", "test-topic-2");
        for (int i = 0; i < 100; i++) {
            messageSender.send(topics.get(i % 2), i % 3, "key-" + i, "message-" + i);
        }
    }
}
