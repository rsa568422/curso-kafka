package com.devs4j.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ExampleProducer {

    public static final Logger log = LoggerFactory.getLogger(ExampleProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            final int user1 = 1020;
            final int user2 = 1021;

            producer.send(new ProducerRecord<>("example-topic", String.valueOf(user1), String.valueOf(200)));
            producer.send(new ProducerRecord<>("example-topic", String.valueOf(user1), String.valueOf(100)));
            producer.send(new ProducerRecord<>("example-topic", String.valueOf(user1), String.valueOf(200)));
            producer.send(new ProducerRecord<>("example-topic", String.valueOf(user1), String.valueOf(-300)));
            producer.send(new ProducerRecord<>("example-topic", String.valueOf(user2), String.valueOf(200)));
            producer.send(new ProducerRecord<>("example-topic", String.valueOf(user2), String.valueOf(200)));

            producer.flush();
        }

        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);

    }

}
