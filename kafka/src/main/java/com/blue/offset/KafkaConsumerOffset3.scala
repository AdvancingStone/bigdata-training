package com.blue.offset

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerOffset3 {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092.slave2:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g11")

    //如果系统没有消费者得到偏移量，系统会读取该分区最早的偏移量
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    //offset偏移量自动提交，默认是true
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test2"))
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(1)
      //从队列中取到了数据
      if (!records.isEmpty) {
        val offsetMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
        val iterator: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while (iterator.hasNext) {
          //获取一个消费者消息
          val record: ConsumerRecord[String, String] = iterator.next()
          val topic = record.topic()
          val key = record.key()
          val value = record.value()
          val offset = record.offset()
          val partition = record.partition()

          //记录消费分区的偏移量数据
          offsetMap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1))
          consumer.commitAsync(offsetMap, new OffsetCommitCallback {
            override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
              print(s"offsets: ${offsets}\texception: ${exception}")
            }
          })

          println(s"topic: ${topic}\tkey: ${key}\tvalue: ${value}\toffset: ${offset}\tpartition: ${partition}")
        }
      }
    }
  }

}
