package com.devs4j.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Devs4jProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // broker de kafka al que vamos a conectar
        properties.put("acks", "1"); // 1 para asegurar que un nodo recibe el mensaje, all para que todos lo reciban
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialización
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialización

        try(Producer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>("devs4j-topic", "devs4j-key", "devs4j-value"));
        }

    }

}
