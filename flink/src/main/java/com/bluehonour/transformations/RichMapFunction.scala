package com.bluehonour.transformations

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

object RichMapFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    stream.map(new RichMapFunction[String,String] {
      private var jedis: Jedis = _

      //初始化函数，在每一个thread启动的时候（处理元素的时候，调用一次）
      //在open中可以创建Redis的连接
      override def open(parameters: Configuration): Unit = {
        //getRuntimeContext可以获取flink运行的上下文，AbstractRichFunction抽象类提供的
        val taskName = getRuntimeContext.getTaskName
        val taskNameWithSubtasks = getRuntimeContext.getTaskNameWithSubtasks
        println("=========open======"+"taskName:" + taskName + "\tsubtasks:"+taskNameWithSubtasks)
        jedis = new Jedis("master", 6379)
        jedis.auth("liushuai")
        println(jedis.ping())
        jedis.select(3)
      }

      override def close(): Unit = {
        jedis.quit()
      }
      //没处理一次元素就会调用一次
      override def map(value: String): String = {
        val name = jedis.get(value)
        if(name==null){
          "not found name"
        }else{
          name
        }
      }
    }).setParallelism(2).print()
    env.execute()
  }

}
