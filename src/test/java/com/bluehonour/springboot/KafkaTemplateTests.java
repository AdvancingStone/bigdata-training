package com.bluehonour.springboot;

import com.bluehonour.kafka.springboot.KafkaSpringBootApplication;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = KafkaSpringBootApplication.class)
@RunWith(SpringRunner.class)
public class KafkaTemplateTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void testSendMessage01(){ //非事务下执行
        kafkaTemplate.send(new ProducerRecord<String, String>("test", "001", "this is kafka template"));
    }

    @Test
    public void testSendMessage02(){ //事务环境下执行
        kafkaTemplate.execute(new KafkaOperations.ProducerCallback<String, String, Object>() {
            @Override
            public Object doInKafka(Producer<String, String> producer) {
                producer.send(new ProducerRecord<String, String>("test", "002", "this is kafka transaction"));
                return null;
            }
        });
    }
}
