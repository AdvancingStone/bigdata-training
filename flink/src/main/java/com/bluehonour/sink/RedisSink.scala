package com.bluehonour.sink

import java.net.InetSocketAddress
import java.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
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
    val config = new FlinkJedisPoolConfig.Builder().setHost("master").setDatabase(2).setPassword("xxx").setPort(6379).build()
    //如果redis是集群
//    val addresses = new util.HashSet[InetSocketAddress]()
//    addresses.add(new InetSocketAddress("node01", 6379))
//    addresses.add(new InetSocketAddress("node02", 6379))
//    val clusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(addresses).build()

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
