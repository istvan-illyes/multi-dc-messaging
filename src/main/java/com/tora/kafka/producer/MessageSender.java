package com.tora.kafka.producer;

import java.io.Closeable;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MessageSender implements Closeable {

    private final Producer<String, String> producer;

    public MessageSender(Properties properties) {
        producer = new KafkaProducer<>(properties);
    }

    public void send(String topic, int partition, String key, String message) {
        producer.send(new ProducerRecord<>(topic, partition, key, message), new MessageSendCallback());
    }

    public void close() {
        producer.close();
    }

    static class MessageSendCallback implements Callback {

        private final Logger logger = LogManager.getLogger(MessageSender.class);

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                logger.error("Failed to send record with offset " + metadata.offset(), exception);
            } else {
                // TODO: sync offset to FT pair (to confirm message was successfully sent)
                logger.info("The offset of the record we just sent is: " + metadata.offset());
            }
        }
    }
}
