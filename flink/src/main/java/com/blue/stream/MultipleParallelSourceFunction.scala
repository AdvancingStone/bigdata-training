package com.blue.stream

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.util.Random


object MultipleParallelSourceFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceStream = env.addSource(new ParallelSourceFunction[String]{
      var flag = true
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while(flag){
          ctx.collect("hi"+ random.nextInt(1000))
          Thread.sleep(2000)
        }
      }

      override def cancel(): Unit = {
       flag = false
      }
    }).setParallelism(5)

    sourceStream.print()
    env.execute()
  }

}
