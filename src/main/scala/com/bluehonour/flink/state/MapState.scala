package com.bluehonour.flink.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * MapState<UK, UV>:状态值为一个Map，用户通过put或putAll方法添加元素，get(key)通过指定的
 * key获取value，使用entries()、keys()、values()检索
 * 案例2：使用MapState 统计单词出现次数
 */
object MapState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List("hello spark", "hello flink", "hello kafka"))
    val pairStream = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1)
    pairStream.map(new RichMapFunction[(String, Int), (String, Int)] {
      private var mapState: MapState[String, Int] = _
      override def open(parameters: Configuration): Unit = {
        //定义mapState存储的数据类型
        val desc = new MapStateDescriptor[String, Int]("sum", createTypeInformation[String], createTypeInformation[Int])
        //注册mapState
        mapState = getRuntimeContext.getMapState(desc)
      }

      override def map(value: (String, Int)): (String, Int) = {
        val key = value._1
        val value2 = value._2
//        println(s"${key}\t${value2}")
        if(mapState.contains(key)){
          mapState.put(key, mapState.get(key) + value2)
        }else{
          mapState.put(key, 1)
        }
        val iterator = mapState.keys().iterator()
        while (iterator.hasNext){
          val key = iterator.next()
          println(s"word: ${key}\tcount: ${mapState.get(key)}")
        }
        value
      }
    }).setParallelism(1)
    env.execute()
  }

}
