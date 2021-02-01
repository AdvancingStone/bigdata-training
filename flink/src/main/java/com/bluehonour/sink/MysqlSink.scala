package com.bluehonour.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer

/**
 * 幂等性
 */
object MysqlSink {

  case class CarInfo(monitorId: String, carId: String, eventTime: String, Speed: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka",
      new KafkaDeserializationSchema[(String, String)] {
      override def isEndOfStream(nextElement: (String, String)): Boolean = false

      override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(record.key(), "UTF-8")
        val value = new String(record.value(), "UTF-8")
        (key, value)
      }

      //指定返回的数据类型， Flink提供的类型
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, props))

    stream.map(data=>{
      val value = data._2
      val split = value.split("\t")
      val monitorId = split(0)
      (monitorId, 1)
    }).keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2+value2._2)
        }
      }).addSink(new MySQLCustomSink)
    env.execute()
  }

  class MySQLCustomSink extends RichSinkFunction[(String, Int)]{
    var conn: Connection = _
    var insertPst: PreparedStatement = _
    var updatePst: PreparedStatement = _

    //thread初始化的时候执行一次
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://master:3306/test", "root", "liushuai")
      insertPst = conn.prepareStatement("insert into car_flow(monitorId, count) values(?, ?)")
      updatePst = conn.prepareStatement("update car_flow set count = ? where monitorId = ?")
    }

    //每来一个元素都会调用一次
    override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
      println(value)
      updatePst.setInt(1, value._2)
      updatePst.setString(2, value._1)
      updatePst.execute()
      println(updatePst.getUpdateCount)
      if(updatePst.getUpdateCount == 0){
        println("Insert")
        insertPst.setString(1, value._1)
        insertPst.setInt(2, value._2)
        insertPst.execute()
      }
    }
    override def close(): Unit = {
      insertPst.close()
      updatePst.close()
      conn.close()
    }
  }
}
