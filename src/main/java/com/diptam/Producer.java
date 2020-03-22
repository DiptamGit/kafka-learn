package com.diptam;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String TOPIC = "first_topic";

    public static void main(String[] args) {

        Properties producerProp = createKafkaProducerProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProp);

        try {
            producer.send(createProducerRecord("First Message via Java Client"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }

    }

    private static ProducerRecord createProducerRecord(String message) {

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
        return record;
    }

    private static Properties createKafkaProducerProperties() {
        //https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
