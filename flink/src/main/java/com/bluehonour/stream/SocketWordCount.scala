package com.bluehonour.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object SocketWordCount {
  def main(args: Array[String]): Unit = {
    //准备环境
    /**
      * createLocalEnvironment 创建一个本地执行的环境  local
      * createLocalEnvironmentWithWebUI 创建一个本地执行的环境  同时还开启Web UI的查看端口  8081
      * getExecutionEnvironment 根据你执行的环境创建上下文，比如local  cluster
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /**
      * DataStream：一组相同类型的元素 组成的数据流
      */
    val initStream: DataStream[String] = env.socketTextStream("master", 8888)

    val wordStream = initStream.flatMap(_.split("\\s+"))
    val pairStream = wordStream.map((_,1))
    val keyByStream = pairStream.keyBy(0)
    val resultStream = keyByStream.sum(1)
    resultStream.print()

    //启动flink任务
    env.execute("first flink job")



  }
}
