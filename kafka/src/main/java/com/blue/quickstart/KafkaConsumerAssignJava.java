package com.blue.quickstart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerAssignJava {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅相关的topic，手动指定消费分区，失去组管理特性
        List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition("test", 1));
        consumer.assign(topicPartitions);

        //指定消费分区的位置
        //从头开始消费
//        consumer.seekToBeginning(topicPartitions);
        //从最近的offset位置开始消费
        consumer.seekToEnd(topicPartitions);
        //从指定分区的offset开始消费
//        consumer.seek(new TopicPartition("test", 1), 60);

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
