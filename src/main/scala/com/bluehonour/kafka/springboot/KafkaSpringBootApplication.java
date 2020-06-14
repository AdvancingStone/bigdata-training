package com.bluehonour.kafka.springboot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;

@SpringBootApplication
public class KafkaSpringBootApplication {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(KafkaSpringBootApplication.class, args);
        System.in.read();
    }

    @KafkaListeners(value = @KafkaListener(topics = {"test"}))
    public void receive(ConsumerRecord<String, String> record){
        System.out.println("record: "+record);
    }

    @KafkaListeners(value = @KafkaListener(topics = {"test2"}))
    @SendTo("test3")
    public String receive2(ConsumerRecord<String, String> record){
        return record.value()+"\t send to test3";
    }
}
