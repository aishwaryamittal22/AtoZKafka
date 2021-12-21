package com.github.aishwaryamittal22.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        System.out.println("Hey!! I am  consumer Demo class");
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String topic = "first_kafka_topic_unknown";

        //1. Create Consumer properties
        Properties consumerProp = new Properties();
        consumerProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        consumerProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"AToZkafkaConsumer1");
        consumerProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //2. Create the Consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(consumerProp);

        //subscribe consumer to single topic
        kafkaConsumer.subscribe(Collections.singleton(topic));

        //subscribe consumer to multiple topic
        //kafkaConsumer.subscribe(Arrays.asList(topic,"2nd-topic-name"));

        //poll for new data
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for(ConsumerRecord<String, String> record : records){
                logger.info("Key: "+ record.key() + " ,value: "+ record.value() + " ,topic: "+ record.topic());
                logger.info("Partition: " + record.partition() + " ,offset: "+ record.offset());
            }
        }

    }
}
