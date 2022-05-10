package com.blue.transformations

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

object Reducer {
  case class CarFlow(monitor_id:String, car_id:String, event_time:String, speed:Double)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers","master:9092,slave1:9092,slave2:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","flink001")
    props.getProperty("auto.offset.reset","latest")

    val stream = env addSource new FlinkKafkaConsumer[String]("flink_kafka",
      new SimpleStringSchema(), props)

    stream.map(data=>{
      val splists = data.split("\t")
      val carFlow = CarFlow(splists(0), splists(1), splists(2), splists(3).toDouble)
      (carFlow, 1)
    }).keyBy(_._1.monitor_id)
      .sum(1)
      .print()
    env.execute()

  }

}
