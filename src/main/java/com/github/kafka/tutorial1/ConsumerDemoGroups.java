package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * if you have 3 partitions and you run this program 3 times, each program will read data from
 * one partition assigned to it
 */
public class ConsumerDemoGroups {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    public static void main(String[] args) {

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Recieved new metadata: \n"+
                        "Topic: "+record.topic() +"\n"+
                        "Partition: "+record.partition() +"\n"+
                        "Offset: "+record.offset() +"\n"+
                        "Timestamp: "+record.timestamp());
            }
        }
    }
}
