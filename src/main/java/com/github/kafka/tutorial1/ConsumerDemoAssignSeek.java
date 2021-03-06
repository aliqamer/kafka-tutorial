package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * read from specific partition
 */
public class ConsumerDemoAssignSeek {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

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
//        consumer.subscribe(Arrays.asList(topic));

        //assign and seek are mostly used to replay data or fetch a specific message
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 10L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMsgToRead = 5;
        boolean keepReading = true;
        int numberOfMsgRead = 0;

        //poll for new data
        while(keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Recieved new metadata: \n"+
                        "Topic: "+record.topic() +"\n"+
                        "Partition: "+record.partition() +"\n"+
                        "Offset: "+record.offset() +"\n"+
                        "Timestamp: "+record.timestamp());
                numberOfMsgRead++;
                if(numberOfMsgRead >= numberOfMsgToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting application");
    }
}
