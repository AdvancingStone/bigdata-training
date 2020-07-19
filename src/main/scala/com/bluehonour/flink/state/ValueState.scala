package com.bluehonour.flink.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * ValueState:类型为T的单值状态，这个状态与对应的Key绑定，最简单的状态，通过update更新
 * 值，通过value获取状态值
 *
 * 案例1：使用ValueState keyed state检查车辆是否发生了急加速
 */
object ValueState {
  case class CarInfo(carId: String, speed: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    stream.map(data=>{
      val arr = data.split(" ")
      CarInfo(arr(0), arr(1).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, String] {
        //保存上一次车速
        private var lastTempState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val lastTempStateDesc = new ValueStateDescriptor[Long]("lastTempState", createTypeInformation[Long])
          lastTempState = getRuntimeContext.getState(lastTempStateDesc)
        }

        override def map(value: CarInfo): String = {
          val lastSpeed = lastTempState.value()
          this.lastTempState.update(value.speed)
          if((lastSpeed-value.speed).abs > 30 && lastSpeed != 0){
            "Over speed " + value.toString
          }else{
            value.carId
          }
        }
      }).print()
    env.execute()
  }

}
