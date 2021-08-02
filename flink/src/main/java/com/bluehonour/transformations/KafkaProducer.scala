package com.bluehonour.transformations

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.io.Source

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    prop.setProperty("key.serializer", classOf[StringSerializer].getName)
    prop.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](prop)
    val iterator = Source.fromFile("data/carFlow_all_column_test.txt", "UTF-8").getLines()
    for (i <- 1 to 100){
      for (line <- iterator){
        //将需要的字段值生产到kafka集群，car_id, monitor_id, event_time, speed
        val splits = line.split(",")
        val monitor_id = splits(0).replace("'", "")
        val car_id = splits(2).replace("'", "")
        val event_time = splits(4).replace("'", "")
        val speed = splits(6).replace("'", "")
        if(!"00000000".equals(car_id)){
          val event = new StringBuilder
          event.append(monitor_id + "\t").append(car_id + "\t").append(event_time +"\t").append(speed)
          producer.send(new ProducerRecord[String, String]("flink_kafka", event.toString()))
          Thread.sleep(100)
          println(event.toString())
        }


      }
    }



  }

}
