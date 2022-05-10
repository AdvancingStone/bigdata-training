package com.blue.sink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.io.PrintStream
import java.net.{InetAddress, Socket}
import java.util.Properties

object SocketSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", new KafkaDeserializationSchema[(String, String)] {
      override def isEndOfStream(nextElement: (String, String)): Boolean = false

      override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(record.key(), "utf-8")
        val value = new String(record.value(), "utf-8")
        (key, value)
      }

      override def getProducedType: TypeInformation[(String, String)] = {
       createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, props))

    stream.map(data=>{
      val value = data._2
      val splits = value.split("\t")
      val monitorId = splits(0)
      (monitorId, 1)
    }).keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2+value2._2)
        }
      }).addSink(new SocketCustomSink("master", 8888))
    env.execute()
  }

  class SocketCustomSink(host: String, port: Int) extends RichSinkFunction[(String, Int)]{
    var socket: Socket = _
    var writer: PrintStream = _

    override def open(parameters: Configuration): Unit = {
     socket = new Socket(InetAddress.getByName(host), port)
      writer = new PrintStream(socket.getOutputStream)
    }

    override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
     writer.println(value._1 + "\t" + value._2)
      writer.flush()
    }

    override def close(): Unit = {
      writer.close()
      socket.close()
    }
  }
}
