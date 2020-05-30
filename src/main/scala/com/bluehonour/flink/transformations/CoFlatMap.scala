package com.bluehonour.flink.transformations

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.socketTextStream("slave2", 8888)
    val stream2 = env.socketTextStream("slave1", 8888)

    //CoFlatMap第一种实现方式：
//    stream1.connect(stream2).flatMap(
//      //对第一个数据流作计算
//      (x, c:Collector[String])=>{
//      x.split(" ").foreach(w=>{c.collect(w)})
//    },
//      //对第二个数据流作计算
//      (y, c:Collector[String])=>{
//        y.split(" ").foreach(w=>{c.collect(w)})
//      }
//    ).print()

    //CoFlatMap第二种实现方式：
//    stream1.connect(stream2).flatMap(
//      //对第一个数据流作计算
//      x => x.split(" "),
//      //对第二个数据流作计算
//      y => y.split(" ")
//    ).print()

    //CoFlatMap第三种实现方式：
    stream1.connect(stream2).flatMap(new CoFlatMapFunction[String,String,(String,Int)] {
      override def flatMap1(value: String, out: Collector[(String, Int)]): Unit = {
        value.split(" ").foreach(w=>{
          out.collect((w,1))
        })
      }

      override def flatMap2(value: String, out: Collector[(String, Int)]): Unit = {
        value.split(" ").foreach(w=>{
          out.collect((w,1))
        })
      }
    }).print()

    env.execute()
  }
}
