package com.blue.interceptors

import java.util.Properties

import com.blue.kafka.interceptors.UserDefineProducerInterceptor
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducerInterceptor {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, classOf[UserDefineProducerInterceptor].getName)

    val producer = new KafkaProducer[String, String](props)

    for (i <- 0 to 5){
      val record = new ProducerRecord[String, String]("test", "key"+i, "value"+i)
      producer.send(record)
    }

    producer.close()


  }

}
