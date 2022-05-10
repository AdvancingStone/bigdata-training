package com.blue.dml;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicDmlJava {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建kafkaAdminClient
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9093,slave1:9092,slave2:9092");
        AdminClient adminClient = KafkaAdminClient.create(props);

        //创建topic信息,默认是异步创建
//        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic("test3", 3, (short) 3)));
//        createTopicsResult.all().get(); //同步创建

        //删除topic,默认是异步删除
//        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList( "topic2"));
//        deleteTopicsResult.all().get();  //同步删除

        //查看topic列表
//        ListTopicsResult topicResult = adminClient.listTopics();
//        Set<String> names = topicResult.names().get();
//        for (String name : names) {
//            System.out.println(name);
//        }

        //查看topic的详细信息
//        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("test2"));
//        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
//        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
//            System.out.println(entry.getKey() + "\t" + entry.getValue());
//        }

        //关闭AdminClient
        adminClient.close();
    }
}
