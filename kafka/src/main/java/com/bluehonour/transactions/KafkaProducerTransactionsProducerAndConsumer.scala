package com.bluehonour.transactions

import java.util
import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


object KafkaProducerTransactionsProducerAndConsumer {
  val GROUP_ID = "g1"
  val SOURCE_TOPIC_NAME = "test2"
  val SINK_TOPIC_NAME  = "test"

  def main(args: Array[String]): Unit = {
    val producer = buildKafkaProducer[String, String]
    val consumer = buildKafkaConsumer[String, String](GROUP_ID)
    //初始化事务
    producer.initTransactions()
    consumer.subscribe(util.Arrays.asList(SOURCE_TOPIC_NAME))
    while(true){
      val consumerRecords: ConsumerRecords[String, String] = consumer.poll(1)
      if(!consumerRecords.isEmpty){
        val recordIterator: util.Iterator[ConsumerRecord[String, String]] = consumerRecords.iterator()
        val map = new util.HashMap[TopicPartition, OffsetAndMetadata]()
        //开启事务控制
        producer.beginTransaction()
        try {
          //迭代数据，进行数据处理
          while (recordIterator.hasNext){
            val record = recordIterator.next()
            //存储元数据
            map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1))
            val pRecord = new ProducerRecord[String, String](SINK_TOPIC_NAME, record.partition(),
              record.timestamp(), record.key(), record.value() + "...我消费到了", record.headers())
            producer.send(pRecord)
          }
          //提交事务
          producer.sendOffsetsToTransaction(map, GROUP_ID)
          producer.commitTransaction()
        } catch {
          case e : Exception => {
            println("出错了~ "+e.getMessage)
            //终止事务
            producer.abortTransaction()
          }
        } finally {
//          consumer.close()
//          producer.close()
        }
      }

    }
  }

  def buildKafkaProducer[p, v] = {
    //创建kafkaProducer
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //必须配置事务id, 必须是唯一的
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactions-id"+UUID.randomUUID().toString)
    //配置kafka批处理大小
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1024")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")

    //配置kafka的重试机制和幂等性
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")

    new KafkaProducer[p, v](props)
  }

  def buildKafkaConsumer[k, v](groupid: String) = {
    //创建kafkaConsumer
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)

    //设置消费者的消费事务的隔离级别 read_committed
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    //必须关闭消费者端的 offset 自动提交
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    new KafkaConsumer[k, v](props)


  }

}
