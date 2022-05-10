package com.blue.offset

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerOffset4 {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092.slave2:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "g439")

    //如果系统没有消费者得到偏移量，系统会读取该分区最早的偏移量
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    //配置offset自动提交的时间
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000")
    //offset偏移量自动提交，默认是true
//    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, boolean2Boolean(true))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test2"))
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
