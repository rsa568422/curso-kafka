package com.devs4j.kafka.callbacks;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Devs4jCallbackProducer {

    public static final Logger log = LoggerFactory.getLogger(Devs4jCallbackProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 10000; i++) {
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"),
                        (recordMetadata, exception) -> {
                            if (exception != null) {
                                log.error("There was an error {}", exception.getMessage());
                            }
                            log.info("Offset = {}, Partition = {}, Topic = {}",
                                    recordMetadata.offset(),
                                    recordMetadata.partition(),
                                    recordMetadata.topic());
                        });

                /*producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"),
                        new Devs4jCallback());*/
            }
            producer.flush();
        }

        log.info("Processing time = {} ms", System.currentTimeMillis() - startTime);

    }

}

class Devs4jCallback implements Callback {

    public static final Logger log = LoggerFactory.getLogger(Devs4jCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception != null) {
            log.error("There was an error {}", exception.getMessage());
        }
        log.info("Offset = {}, Partition = {}, Topic = {}",
                recordMetadata.offset(),
                recordMetadata.partition(),
                recordMetadata.topic());
    }
}
