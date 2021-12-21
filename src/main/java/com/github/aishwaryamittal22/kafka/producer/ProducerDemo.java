package com.github.aishwaryamittal22.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        System.out.println("Hey!! I am  producer Demo class");

        //1. Create Producer properties
        Properties producerProp = new Properties();
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2. Create the Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(producerProp);

        //3a. Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_kafka_topic_unknown","Hello-first-topic");

        //3b. send data - asynchronous
        kafkaProducer.send(record);

        //flush data
        kafkaProducer.flush();

        //flush data and close producer
        kafkaProducer.close();
    }
}
