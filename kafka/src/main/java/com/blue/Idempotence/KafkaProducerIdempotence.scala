package com.blue.Idempotence

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducerIdempotence {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //设置kafka的acks和retries
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    //不包含第一次发送，如果系统尝试三次失败，则系统放弃发送
    props.put(ProducerConfig.RETRIES_CONFIG, "5")
    //将检测时间设置为7ms
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "7")
    //开启kafka的幂等性
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    //保证严格有序
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")

    val producer = new KafkaProducer[String, String](props)

    val record = new ProducerRecord[String, String]("test3", "idempotence", "test idempotence")
    producer.send(record)
    producer.flush()

    producer.close()


  }

}
