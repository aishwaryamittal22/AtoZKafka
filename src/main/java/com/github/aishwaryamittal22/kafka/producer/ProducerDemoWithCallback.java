package com.github.aishwaryamittal22.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        //1. Create Producer properties
        Properties producerProp = new Properties();
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2. Create the Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(producerProp);


        //3b. send data with callback method - asynchronous
        for(int i=0;i<10;i++) {
            //3a. Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_kafka_topic","Hello-first-topic"+Integer.toString(i));

            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //successfully record is sent
                        logger.info("Received record with following metadata : topic: {}, partition: {}, offset: {} and timestamp: {}"
                                , recordMetadata.topic()
                                , recordMetadata.partition()
                                , recordMetadata.offset()
                                , recordMetadata.timestamp());
                    } else {
                        // if exception is occurred
                        logger.error("Error while producing the record", e);
                    }
                }
            });
        }
        //flush data
        kafkaProducer.flush();

        //flush data and close producer
        kafkaProducer.close();
    }
}
