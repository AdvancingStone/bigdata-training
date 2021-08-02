package com.bluehonour.stream

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object FileSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "hdfs://master:9000/flink/data"
    val textInputFormat = new TextInputFormat(new Path(filePath))
    val textStream = env.readFile(textInputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
    textStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()
    env.execute()
  }

}
