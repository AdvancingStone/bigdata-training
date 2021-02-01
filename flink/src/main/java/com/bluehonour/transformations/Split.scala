package com.bluehonour.transformations

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Split {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 100)
    val splitStream = stream.split(x=>{
      x%2 match {
        case 0 => List("even")
        case 1 => List("odd")
      }
    })
    splitStream.select("even").print()
    env.execute()

  }

}
