package com.bluehonour.flink.stream

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val initDS: DataSet[String] = env.readTextFile("hdfs://master:9000/flink/data/wc")
    val resultDS: AggregateDataSet[(String, Int)] = initDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    resultDS.print()
  }

}
