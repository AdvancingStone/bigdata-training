package com.bluehonour.flink.transformations

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessFunction {
  case class CarInfo(carId: String, speed: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master", 8888)
    stream.map(x=>{
      val splits = x.split("\\s+")
      val carId = splits(0)
      val speed = splits(1).toLong
      CarInfo(carId, speed.toLong)
    }).keyBy(_.carId)
      //KeyedStream调用process需要传入KeyedProcessFunction
      //DataStream调用process需要传入ProcessFunction
      .process(new KeyedProcessFunction[String, CarInfo, String] {

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CarInfo, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        val warnMsg = "warn... time:" + timestamp + " carID:" + ctx.getCurrentKey
        out.collect(warnMsg)

      }

      override def processElement(value: CarInfo,
                                  ctx: KeyedProcessFunction[String, CarInfo, String]#Context,
                                  out: Collector[String]): Unit = {
        val currentTime = ctx.timerService().currentProcessingTime()
        println(ctx.getCurrentKey)  //获取key，也就是CarInfo的key
        if(value.speed>100){
          //监控每辆汽车，车速超过100迈，2s钟后发出超速的警告通知
          val timerTime = currentTime + 2*1000
          ctx.timerService().registerProcessingTimeTimer(timerTime)
        }


      }
    }).print()
    env.execute()
  }

}
