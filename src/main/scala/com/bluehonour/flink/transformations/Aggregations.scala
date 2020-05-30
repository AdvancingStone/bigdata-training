package com.bluehonour.flink.transformations

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.common.serialization.StringSerializer


object Aggregations {
  case class CarFlow(monitor_id:String, car_id:String, event_time:String, speed:Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    prop.setProperty("key.serializer", classOf[StringSerializer].getName)
    prop.setProperty("value.serializer", classOf[StringSerializer].getName)
    prop.setProperty("group.id", "flink004")
    prop.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka",
      new SimpleStringSchema(), prop))
    stream.map(data=>{
      val splits = data.split("\t")
      val carFlow = CarFlow(splits(0), splits(1), splits(2), splits(3).toDouble)
      val event_time = carFlow.event_time
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = dateFormat.parse(event_time)
      (carFlow, date)
    }).keyBy(_._1.monitor_id)
      .min(1)
      .map(_._1)
      .print()
    env.execute()
  }

}
