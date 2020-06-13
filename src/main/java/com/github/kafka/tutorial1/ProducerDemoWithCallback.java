package com.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 10; i++) {
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello from java: "+i);
            //send data async
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //execute everytime record is sent successfully or error
                    if(exception == null) {
                        logger.info("Recieved new metadata: \n"+
                                "Topic: "+metadata.topic() +"\n"+
                                "Partition: "+metadata.partition() +"\n"+
                                "Offset: "+metadata.offset() +"\n"+
                                "Timestamp: "+metadata.timestamp());
                    } else {
                        logger.error("Error while producing: "+ exception.getStackTrace());
                    }
                }
            });
        }

        kafkaProducer.flush();
        kafkaProducer.close();

        System.out.println("success");
    }
}

