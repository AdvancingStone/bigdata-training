package com.bluehonour.serializer

import java.util
import java.util.Properties

import com.bluehonour.kafka.serializer.UserDefineDeserializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerUser {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092.slave2:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[UserDefineDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g8")

    val consumer = new KafkaConsumer[String, User](props)
    consumer.subscribe(util.Arrays.asList("test"))
    while(true){
      val records: ConsumerRecords[String, User] = consumer.poll(1)
      if(!records.isEmpty){
        val iterator: util.Iterator[ConsumerRecord[String, User]] = records.iterator()
        while (iterator.hasNext){
          val record: ConsumerRecord[String, User] = iterator.next()
          val key = record.key()
          val value = record.value()

          println(s"key: ${key}\tvalue: ${value}\t")
        }
      }
    }
  }

}
