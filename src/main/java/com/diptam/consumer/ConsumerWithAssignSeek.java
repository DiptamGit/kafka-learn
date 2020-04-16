package com.diptam.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithAssignSeek {

    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String TOPIC = "first_topic";

    final static Logger logger = LoggerFactory.getLogger(ConsumerWithAssignSeek.class);

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createKafkaConsumerProperties());
        TopicPartition topicPartitionReadFrom = new TopicPartition(TOPIC,0);
        Long offsetToRead = 2L;

        consumer.assign(Arrays.asList(topicPartitionReadFrom));
        consumer.seek(topicPartitionReadFrom, offsetToRead);

        try {
            int counter = 5;
            boolean readMessage = true;
            while (readMessage){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                records.forEach(record -> logger.info("****** Record Data ****** : "+record.key()+" :::: "+record.value()+" :::: "+record.offset()));
                counter--;
                if(counter == 0)
                    break;
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

        return properties;
    }
}
