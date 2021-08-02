package com.bluehonour.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer

import java.text.SimpleDateFormat
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * ListState：Key上的状态值为一个列表，这个列表可以通过add方法往列表中添加值，
 * 也可以通过get()方法返回一个Iterable来遍历状态值
 * 案例5：统计每辆车的运行轨迹
 * 所谓运行轨迹就是这辆车的信息按照时间排序，卡口号串联起来
 */
object ListState {
  case class CarInfo(monitorId: String, carId: String, eventTime: Long, speed: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    stream.map(data=>{
      val arr = data.split(" ")
      //卡口、车牌、事件时间、车速
      CarInfo(arr(0), arr(1), arr(2).toLong, arr(3).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, (String, String)] {
        //eventTime monitorId
        private var listState: ListState[(Long, String)] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ListStateDescriptor[(Long, String)]("listState", createTypeInformation[(Long, String)])
          listState = getRuntimeContext.getListState(desc)
        }

        override def map(value: CarInfo): (String, String) = {
          listState.add(value.eventTime, value.monitorId)
          val seq = listState.get().asScala.seq
          val sortList = seq.toList.sortBy(x => x._1)
          val builder = new StringBuilder
          for (elem <- sortList){
            builder.append(elem._2 + "\t")
          }
          (value.carId, builder.toString())
        }
      }).print()
    env.execute()
  }
}
