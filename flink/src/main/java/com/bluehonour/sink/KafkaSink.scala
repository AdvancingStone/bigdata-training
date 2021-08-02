package com.bluehonour.sink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.util.Properties

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    val result: DataStream[(String, Int)] = stream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
//    props.setProperty("key.serializer", classOf[StringSerializer].getName)
//    props.setProperty("value.serializer", classOf[StringSerializer].getName)

    result.addSink(new FlinkKafkaProducer[(String, Int)]("wc", new KafkaSerializationSchema[(String, Int)] {
      override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord("wc", element._1.getBytes(), (element._2+"").getBytes())
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    env.execute()
  }

}
