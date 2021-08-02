package com.bluehonour.state

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * ReducingState：每次调用add()方法添加值的时候，会调用用户传入的reduceFunction，最后合并到一个单一的状态值
 * 案例3：使用ReducingState统计每辆车的速度总和
 */
object ReducingState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    stream.map(data=>{
      val arr = data.split(" ")
      CarInfo(arr(0), arr(1).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, CarInfo] {
        private var reducingState: ReducingState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ReducingStateDescriptor[Long]("reduceSpeed", new ReduceFunction[Long] {
            override def reduce(value1: Long, value2: Long): Long = {
              value1 + value2
            }
          }, createTypeInformation[Long])
          reducingState = getRuntimeContext.getReducingState(desc)
        }

        override def map(value: CarInfo): CarInfo = {
          reducingState.add(value.speed)
          println(s"carId: ${value.carId}\tspeed count: ${reducingState.get()}")
          value
        }
      })
    env.execute()
  }

}
