package com.devs4j.kafka.transactional;

import com.devs4j.kafka.producers.Devs4jProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TransactionalConsumer {

    public static final Logger log = LoggerFactory.getLogger(Devs4jProducer.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","devs4j-group");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("isolation.level","read_committed");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Offset = {}, Partition = {}, Key = {}, Value = {}",
                            consumerRecord.offset(),
                            consumerRecord.partition(),
                            consumerRecord.key(),
                            consumerRecord.value());
                }
            }
        }
    }

}
