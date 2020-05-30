package com.bluehonour.flink.transformations

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object CoMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.socketTextStream("slave2", 8888)
    val stream2 = env.socketTextStream("slave1", 8888)
    val wcStream1 = stream1.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    val wcStream2 = stream2.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    val connStream: ConnectedStreams[(String, Int), (String, Int)] = wcStream1.connect(wcStream2)

    //CoMap的第一种实现方式：
//    connStream.map(new CoMapFunction[(String, Int), (String, Int), (String, Int)] {
//      override def map1(value: (String, Int)): (String, Int) = {
//        (value._1+" first: ", value._2+100)
//      }
//
//      override def map2(value: (String, Int)): (String, Int) = {
//        (value._1+" second:", value._2*2)
//      }
//    }).print()

    //CoMap的第二种实现方式
    connStream.map(
      x=>{(x._1+": first", x._2)},
      y=>{(y._1+": second", y._2)}
    ).print()






    env.execute()
  }

}
