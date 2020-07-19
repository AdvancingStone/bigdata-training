package com.bluehonour.flink.state

import com.bluehonour.flink.state.ValueState.CarInfo
import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * AggregatingState<IN, OUT> :保留一个单值，表示添加到状态的所有值的聚合。
 * 和 ReducingState 相反的是, 聚合类型可能与添加到状态的元素的类型不同。
 * 使用add(IN) 添加的元素会调用用户指定的AggregateFunction 进行聚合。
 * 案例4：使用AggregatingState统计每辆车的速度总和
 */
object AggregatingState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    stream.map(data=>{
      val arr = data.split(" ")
      CarInfo(arr(0), arr(1).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, CarInfo] {
        private var aggregatingState: AggregatingState[Long, Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new AggregatingStateDescriptor[Long, Long, Long]("agg", new AggregateFunction[Long, Long, Long] {
            //初始化累加器
            override def createAccumulator(): Long = 0
            //往累加器中累加值
            override def add(value: Long, accumulator: Long): Long = value + accumulator
            //返回最终结果
            override def getResult(accumulator: Long): Long = accumulator
            //合并两个累加器的值
            override def merge(a: Long, b: Long): Long = a + b
          }, createTypeInformation[Long])
          aggregatingState = getRuntimeContext.getAggregatingState(desc)
        }

        override def map(value: CarInfo): CarInfo = {
          aggregatingState.add(value.speed)
          println(s"carId: ${value.carId}\tspeed count: ${aggregatingState.get()}")
          value
        }
      } )
    env.execute()
  }
}
