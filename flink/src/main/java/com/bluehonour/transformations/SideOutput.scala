package com.bluehonour.transformations

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object SideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    val gtTag = new OutputTag[String]("gt")
    val processStream = stream.process(new ProcessFunction[String, String] {
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        try {
          val longVar = value.toLong
          if (longVar > 100) {
            out.collect(value)
          } else {
            ctx.output(gtTag, value)
          }
        } catch {
          case e => e.getMessage
            ctx.output(gtTag, value)
        }
      }
    })
    val sideStream = processStream.getSideOutput(gtTag)
    sideStream.print("sideStream")
    processStream.print("mainStream")
    env.execute()

  }

}
