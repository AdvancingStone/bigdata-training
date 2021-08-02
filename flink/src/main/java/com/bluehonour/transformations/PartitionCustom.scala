package com.bluehonour.transformations

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object PartitionCustom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val stream = env.generateSequence(1,100).map((_,1))
    stream.writeAsText("data/stream1")
    stream.partitionCustom(new Partitioner[Long] {
      override def partition(key: Long, numPartitions: Int): Int = {
        key.toInt % numPartitions
      }
    }, 0).writeAsText("data/stream2").setParallelism(4)
    env.execute()
  }

}
