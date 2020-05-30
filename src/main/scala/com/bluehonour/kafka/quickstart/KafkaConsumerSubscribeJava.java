package com.bluehonour.kafka.quickstart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaConsumerSubscribeJava {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));

        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1);
            if(!consumerRecords.isEmpty()){
                Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                while (iterator.hasNext()){
                    ConsumerRecord<String, String> record = iterator.next();
                    String topic = record.topic();
                    Headers headers = record.headers();
                    String key = record.key();
                    String value = record.value();
                    long offset = record.offset();
                    long timestamp = record.timestamp();
                    int partition = record.partition();

                    System.out.println("topic: "+topic+"\theaders: "+headers+"\tkey: "+key+"\tvalue: "+value+"\toffset: "+offset+"\ttimestamp: "+timestamp+"\tpartition: "+partition);

                }
            }

        }
    }
}
