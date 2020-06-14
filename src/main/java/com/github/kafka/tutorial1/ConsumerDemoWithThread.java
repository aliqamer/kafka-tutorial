package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread() {}

    public void run() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread");
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);

        //start the thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();

            try {
                latch.await();
            }catch (InterruptedException e) {
                logger.info("Application exited!");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted ",e);
        } finally {
            logger.info("Application is closing");
        }
    }

}

class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;
    private static Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
    //create consumer
    KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {

        this.latch = latch;
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<String, String>(properties);
        //subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try{
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
        }catch (WakeupException exception) {
            logger.info("Recieved shutdown signal!");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        //special method to interrupt consumer.poll() it will throw exception WakeUpException
        consumer.wakeup();
    }


}
