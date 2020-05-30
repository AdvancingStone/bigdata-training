package com.bluehonour.kafka.dml

import java.util
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic, TopicDescription}
import scala.collection.JavaConversions._


object KafkaTopicDmlScala {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    val adminClient = AdminClient.create(props)

    //创建topic信息,默认是异步创建
    val createTopics = adminClient.createTopics(List(new NewTopic("topic3", 1, 1)))
    createTopics.all().get()//同步创建

    //删除topic,默认是异步删除
    adminClient.deleteTopics(List("topic3")).all().get()//同步删除

    //查看topic列表
    val listTopics = adminClient.listTopics()
    val names: util.Set[String] = listTopics.names().get()
    names.foreach(println(_))

    //查看topic的详细信息
    val result = adminClient.describeTopics(List("test"))
    val topicDescriptionMap: util.Map[String, TopicDescription] = result.all().get()
    topicDescriptionMap.entrySet().foreach(x => {
      println(s"${x.getKey}\t${x.getValue}")
    })

    adminClient.close()
  }
}
