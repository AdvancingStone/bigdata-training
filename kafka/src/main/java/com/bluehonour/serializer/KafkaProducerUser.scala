package com.bluehonour.serializer

import java.util.{Date, Properties}

import com.bluehonour.kafka.serializer.UserDefineSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducerUser {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[UserDefineSerializer].getName)

    val producer = new KafkaProducer[String, User](props)

    for (i <- 0 to 5){
      val record = new ProducerRecord[String, User]("test", "key"+i, new User(i, s"user${i}", new Date))
      producer.send(record)
    }

    producer.close()


  }

}
