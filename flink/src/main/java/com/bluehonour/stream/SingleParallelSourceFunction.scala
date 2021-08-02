package com.bluehonour.stream

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import java.util.Random

object SingleParallelSourceFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //source 的并行度为1， 单并行度source源
    val stream: DataStream[String] = env.addSource(new SourceFunction[String] {
      var flag = true

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random
        while (flag) {
          ctx.collect("hello" + random.nextInt(1000))
//          ctx.collectWithTimestamp("hi" + random.nextInt(1000) , System.currentTimeMillis())
          Thread.sleep(200)
        }

      }
      //停止生产数据
      override def cancel(): Unit = flag = false
    })
    stream.print()
    env.execute()
  }

}
