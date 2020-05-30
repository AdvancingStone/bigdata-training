package com.bluehonour.flink.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    val result = stream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
    //若redis是单机
    val config = new FlinkJedisPoolConfig.Builder().setHost("master").setDatabase(2).setPassword("liushuai").setPort(6379).build()
    result.addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "wc")
      }

      override def getKeyFromData(data: (String, Int)): String = {
        println("getKeyFromData "+data._1)
        data._1
      }

      override def getValueFromData(data: (String, Int)): String = {
        println("getValueFromData "+data._2+"")
        data._2+""
      }
    }))
    env.execute()
  }

}
