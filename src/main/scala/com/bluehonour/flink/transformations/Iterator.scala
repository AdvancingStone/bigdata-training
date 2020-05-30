package com.bluehonour.flink.transformations

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Iterator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    val iterStream = stream.map(_.toLong).iterate{
      iteration =>{
        //定义迭代逻辑
        val iterationBody = iteration.map(x=>{
          println(s"now ${x}")
          if(x>0) x-1
          else x
        })
        //> 0 大于0的值继续返回到stream流中,当<= 0 继续往下游发送
        (iterationBody.filter(_>0), iterationBody.filter(_<=0))
      }
    }.print()
    env.execute()
  }

}
