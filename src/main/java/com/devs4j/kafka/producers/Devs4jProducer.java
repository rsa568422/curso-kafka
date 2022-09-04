package com.devs4j.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Devs4jProducer {

    public static final Logger log = LoggerFactory.getLogger(Devs4jProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // broker de kafka al que vamos a conectar
        properties.put("acks", "1"); // 1 para asegurar que un nodo recibe el mensaje, all para que todos lo reciban
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialización
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialización
        properties.put("linger.ms", "10"); // serialización

        try(Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 1000000; i++) {
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value")).get();
            }
            producer.flush();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Message producer interrupted ", e);
        }

        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);

    }

}
