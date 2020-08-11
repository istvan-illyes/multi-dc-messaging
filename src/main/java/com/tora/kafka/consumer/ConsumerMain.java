package com.tora.kafka.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ConsumerMain {

    private static final Logger LOGGER = LogManager.getLogger(ConsumerMain.class);

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final MessageReceiver messageReceiver = new MessageReceiver(properties);
        final List<String> topics = Arrays.asList("test-topic-1", "test-topic-2");
        messageReceiver.subscribe(topics);

        while (true) {
            LOGGER.info("Waiting for data...");
            final ConsumerRecords<String, String> records = messageReceiver.receive();
            for (final ConsumerRecord<String, String> record : records) {
                LOGGER.info("Received message with offset " + record.offset() + " key " + record.key() + " value " + record.value());
            }
        }
    }
}
