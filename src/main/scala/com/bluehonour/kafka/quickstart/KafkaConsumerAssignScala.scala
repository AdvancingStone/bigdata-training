package com.bluehonour.kafka.quickstart

import java.util
import java.util.{Arrays, List, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerAssignScala {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092.slave2:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val consumer = new KafkaConsumer[String, String](props)

    //订阅相关的topic，手动指定消费分区，失去组管理特性
    val topicPartitions = util.Arrays.asList(new TopicPartition("test", 0))
    consumer.assign(topicPartitions)

    //指定消费分区的位置
    //从头开始消费
    //        consumer.seekToBeginning(topicPartitions);
    //从最近的offset位置开始消费
    //        consumer.seekToEnd(topicPartitions);
    //从指定分区的offset开始消费
    consumer.seek(new TopicPartition("test", 0), 30)

    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(1)
      if(!records.isEmpty){
        val iterator: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while (iterator.hasNext){
          val record: ConsumerRecord[String, String] = iterator.next()
          val headers = record.headers()
          val topic = record.topic()
          val key = record.key()
          val value = record.value()
          val offset = record.offset()
          val timestamp = record.timestamp()
          val partition = record.partition()

          println(s"headers: ${headers}\ttopic: ${topic}\tkey: ${key}\tvalue: ${value}\toffset: ${offset}\ttimestamp: ${timestamp}\tpartition: ${partition}")
        }
      }
    }
  }

}
