package com.devs4j.kafka.consumers;

import com.devs4j.kafka.producers.Devs4jProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ExampleConsumer {

    public static final Logger log = LoggerFactory.getLogger(ExampleConsumer.class);

    private final static Map<Integer, Integer> CLIENT_CREDIT_MAP = new HashMap<>();

    public static void main(String[] args) {

        CLIENT_CREDIT_MAP.put(1020, 0);
        CLIENT_CREDIT_MAP.put(1021, 0);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","example-group");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("example-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    Long offset = consumerRecord.offset();
                    int partition = consumerRecord.partition();
                    int key = Integer.parseInt(consumerRecord.key());
                    int value = Integer.parseInt(consumerRecord.value());

                    log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", offset, partition, key, value);
                    CLIENT_CREDIT_MAP.replace(key, CLIENT_CREDIT_MAP.get(key) + value);

                    log.info("Client {} -> actual credit = {}", key, CLIENT_CREDIT_MAP.get(key));
                }
            }
        }
    }

}
