package com.devs4j.kafka.transactional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TransactionalProducer {

    public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("transactional.id", "devs4j-producer-id");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.initTransactions();
            producer.beginTransaction();

            for (int i = 0; i < 1000000; i++) {
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"));
            }

            producer.commitTransaction();
            producer.flush();
        }

        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);

    }

}

