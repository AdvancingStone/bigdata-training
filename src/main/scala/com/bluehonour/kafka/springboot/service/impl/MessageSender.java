package com.bluehonour.kafka.springboot.service.impl;

import com.bluehonour.kafka.springboot.service.IMessageSender;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
//@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class })
//@EnableTransactionManagement
//@Transactional
public class MessageSender implements IMessageSender {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void sendMessage(String topic, String key, String message) {
        kafkaTemplate.send(new ProducerRecord<String, String>(topic, key, message));
    }
}
