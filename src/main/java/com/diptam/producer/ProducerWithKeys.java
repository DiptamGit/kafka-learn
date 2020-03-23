package com.diptam.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String TOPIC = "first_topic";

    final static Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(createKafkaProducerProperties());
        try {
            for (int i = 0; i < 5; i++) {
                sendMessageViaProducer(producer, createProducerRecord("Message via Java Client - with keys & callback - Num ", i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void sendMessageViaProducer(KafkaProducer<String, String> producer, final ProducerRecord record) {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    logger.info("Sending Record : "+record);
                    logger.info("**Record Meta Data**"+"\n"
                            +"Topic : "+recordMetadata.topic()+"\n"
                            +"Partition : "+recordMetadata.partition()+"\n"
                            +"Offset : "+recordMetadata.offset()+"\n"
                            +"Timestamp : "+recordMetadata.timestamp());
                }
            });
    }

    private static ProducerRecord createProducerRecord(String message, int counter) {

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key "+counter,message+counter);
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
