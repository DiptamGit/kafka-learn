package com.diptam.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String TOPIC = "first_topic";
    final static String GROUP_ID = "kafka-local-consumer-group-1";
    final static String OFFSET_TYPE = "earliest"; //latest - earliest - none

    final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createKafkaConsumerProperties());
        consumer.subscribe(Collections.singletonList(TOPIC));
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                records.forEach(record -> logger.info("****** Record Data ****** : "+record.key()+" :::: "+record.value()+" :::: "+record.offset()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

    private static Properties createKafkaConsumerProperties() {
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_TYPE);

        return properties;
    }
}
