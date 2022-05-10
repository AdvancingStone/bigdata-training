package com.blue.sink

import akka.remote.serialization.StringSerializer
import com.blue.utils.Utils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.util.{Date, Properties}


/**
 * 计算结果写入sink 两种实现方式：
 * 1. map算子写入频繁创建hbase连接(不好)
 * 2. process写入适合批量写入hbase
 */
object HBaseSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    stream.map(row => {
      val arr = row.split("\t")
      (arr(0), 1)
    }).keyBy(_._1)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      }).process(new ProcessFunction[(String, Int), (String, Int)] {
      var htable: Table = _

      override def open(parameters: Configuration): Unit = {
        val configuration = HBaseConfiguration.create()
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181")
        //实现创建 create 'car_flow',{NAME => 'count', VERSIONS => 1}
        val tableName = TableName.valueOf("car_flow")
        val connection = ConnectionFactory.createConnection(configuration)
        htable = connection.getTable(tableName)
      }

      override def close(): Unit = {
        htable.close()
      }

      override def processElement(value: (String, Int),
                                  ctx: ProcessFunction[(String, Int), (String, Int)]#Context,
                                  out: Collector[(String, Int)]): Unit = {
        // rowkey: monitorid 时间戳（分钟）  value：车流量
        val minDate = Utils.getDateMin(new Date())
        val put = new Put(Bytes.toBytes(value._1))
        put.addColumn(Bytes.toBytes("count"), Bytes.toBytes(minDate), Bytes.toBytes(value._2))
        htable.put(put)
      }
    })
    env.execute()
  }
}
