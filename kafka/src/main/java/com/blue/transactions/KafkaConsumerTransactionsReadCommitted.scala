package com.blue.transactions

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerTransactionsReadCommitted {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092.slave2:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g32")

    //设置消费者的消费事务的隔离级别 read_committed
    props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test"))
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
