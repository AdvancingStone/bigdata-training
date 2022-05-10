package com.blue.transactions

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object KafkaProducerTransactionsProducerOnly {

  val TOPIC_NAME = "test2"

  def main(args: Array[String]): Unit = {
    val producer = buildKafkaProducer

    //初始化事务
    producer.initTransactions()
    try {
      //开启事务
      producer.beginTransaction()
      for (i <- 0 to 10){
        if(i==8){
          val j = i/0
        }
        val record = new ProducerRecord[String, String](TOPIC_NAME, "error"+i, "value"+i)
        producer.send(record)
        producer.flush()
      }
      //提交事务
      producer.commitTransaction()
    } catch {
      case e : Exception => {
        println("出现错误了： " + e.getMessage)
        //终止事务
        producer.abortTransaction()
      }
    } finally {
      producer.close()
    }
  }

  val buildKafkaProducer = {
    //创建KafkaProducer
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //必须配置事务id，必须是唯一的
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id"+UUID.randomUUID().toString)
    //配置kafka的批处理大小
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1024")
    //等待5ms, 如果batch中的数据不足1024大小
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    //配置kafka重试机制和幂等性
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")

    new KafkaProducer[String, String](props)

  }
}
