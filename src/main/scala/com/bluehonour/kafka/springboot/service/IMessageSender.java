package com.bluehonour.kafka.springboot.service;

public interface IMessageSender {
    public void sendMessage(String topic, String key, String message);
}
