package com.bluehonour.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CollectionSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromCollection(List("hello flink spark", "hi storm spark"))
    stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1).print()
    env.execute()
  }

}
