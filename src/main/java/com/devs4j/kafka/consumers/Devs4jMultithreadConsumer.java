package com.devs4j.kafka.consumers;

import com.devs4j.kafka.multithread.Devs4jThreadConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Devs4jMultithreadConsumer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","devs4j-group");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ExecutorService executor = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 5; i++) {
            Devs4jThreadConsumer consumer = new Devs4jThreadConsumer(new KafkaConsumer<>(properties));
            executor.execute(consumer);
        }

        while (!executor.isTerminated());

    }

}
