package com.diptam.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String TOPIC = "first_topic";

    final static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {

        Properties producerProp = createKafkaProducerProperties();
        for (int i = 0; i < 5; i++) {
            sendMessageViaProducer(producerProp, createProducerRecord("Message via Java Client - with callback - Num "+i));
        }
    }

    private static void sendMessageViaProducer(Properties producerProp, ProducerRecord record) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProp);
        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    logger.info("**Record Meta Data**"+"\n"
                            +"Topic : "+recordMetadata.topic()+"\n"
                            +"Partition : "+recordMetadata.partition()+"\n"
                            +"Offset : "+recordMetadata.offset()+"\n"
                            +"Timestamp : "+recordMetadata.timestamp());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
