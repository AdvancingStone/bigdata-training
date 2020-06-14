package com.bluehonour.springboot;

import com.bluehonour.kafka.springboot.service.IMessageSender;
import com.bluehonour.kafka.springboot.KafkaSpringBootApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = KafkaSpringBootApplication.class)
@RunWith(SpringRunner.class)
public class MessageSenderTests {

    @Autowired
    private IMessageSender iMessageSender;

    @Test
    public void testSendMessage03(){ //事务下执行
        iMessageSender.sendMessage("test", "003", "this is kafka test SendMessage");
    }
}
