package com.blue.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.codehaus.jackson.map.deser.std.StringDeserializer

import java.util.Properties

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    prop.setProperty("group.id", "flink-kafka-id001")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    /**
      * earliest:从头开始消费，旧数据会频繁消费
      * latest:从最近的数据开始消费，不再消费旧数据
      */
    prop.setProperty("auto.offset.reset", "latest")

//    KafkaDeserializationSchema：读取kafka中key、value
//    SimpleStringSchema：读取kafka中value
    val kafkaStream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka",
      new KafkaDeserializationSchema[(String, String)] {
        override def isEndOfStream(nextElement: (String, String)): Boolean = false

        override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
          var key: String = null
          var value: String = null
          if(record.key != null){
            key = new String(record.key, "UTF-8")
          }
          if(record.value != null){
            value = new String(record.value(), "UTF-8")
          }
          print(value)
          (key, value)
        }
        //返回指定类型
        override def getProducedType: TypeInformation[(String, String)] =
          createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }, prop))
    kafkaStream.print()
    env.execute()

  }
}
