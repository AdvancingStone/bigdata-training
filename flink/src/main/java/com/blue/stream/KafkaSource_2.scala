package com.blue.stream

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.codehaus.jackson.map.deser.std.StringDeserializer

import java.util.Properties

object KafkaSource_2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    prop.setProperty("group.id", "flink-kafka-id001")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](
      "flink-kafka", new SimpleStringSchema(), prop))

    kafkaStream.print()
    env.execute()
  }

}
