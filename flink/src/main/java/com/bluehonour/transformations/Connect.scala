package com.bluehonour.transformations

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable

object Connect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val filePath = "data/carId2Name"
    val carId2NameStream  = env.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
    val dataStream = env.socketTextStream("slave1", 8888)
    dataStream.connect(carId2NameStream).map(new CoMapFunction[String, String, String] {
      private val hashmap = new mutable.HashMap[String, String]()

      override def map1(value: String): String = {
        hashmap.getOrElse(value, "not found name")
      }

      override def map2(value: String): String = {
        val splits = value.split(" ")
        hashmap.put(splits(0), splits(1))
        value + " 加载完毕"
      }
    }).print()

    env.execute()

  }

}
