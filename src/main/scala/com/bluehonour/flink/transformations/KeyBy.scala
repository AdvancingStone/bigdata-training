package com.bluehonour.flink.transformations

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object KeyBy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 100)
    //根据索引号来指定分区字段
//    stream.map(x=>(x%3, 1)).keyBy(0).sum(1).print()
    //根据传入匿名函数来指定分区字段
//    stream.map(x=>(x%2, 1)).keyBy(_._1).sum(1).print()
    //通过实现keySelector接口来指定分区字段
    stream.map(x=>(x%2, 1)).keyBy(new KeySelector[(Long, Int),Long] {
      override def getKey(value: (Long, Int)): Long = value._1
    }).sum(1).print()

    env.execute()
  }

}
