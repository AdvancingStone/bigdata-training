package com.bluehonour.crash_test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class MyFunction extends KeyedProcessFunction[Long, (Long, String), String ]{
  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[MyFunction])

  private var keyState: ValueState[Long] = _
  private var valueState: ListState[(Long, String)] = _
  private var date: Date = _

  override def open(parameters: Configuration): Unit = {
    keyState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("key-state", TypeInformation.of(classOf[Long])))

    valueState = getRuntimeContext.getListState(new ListStateDescriptor[(Long, String)]("value-state", TypeInformation.of(classOf[(Long, String)])))
    date = new Date()
  }

  override def processElement(value: (Long, String), ctx: KeyedProcessFunction[Long, (Long, String), String]#Context, out: Collector[String]): Unit = {
    keyState.update(value._1)
    valueState.add(value)
    val timerTime = System.currentTimeMillis() + 3*1000*60
    date.setTime(timerTime)
    val time = new SimpleDateFormat().format(date)
    log.info(s"注册Timer，timestamp：$timerTime")
    log.info(s"下次调用在3分钟后 $time")
    val key = keyState.value()
    val valueSeq = valueState.get().asScala.toSeq
    log.info(s"processElement: ${valueSeq.mkString(",")}")
    ctx.timerService().registerProcessingTimeTimer(timerTime)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (Long, String), String]#OnTimerContext, out: Collector[String]): Unit = {
    date.setTime(timestamp)
    val time = new SimpleDateFormat().format(date)
    log.info(s"开始调用了： ${time} -> 触发Timer：${timestamp}")
    val key = keyState.value()
    val valueSeq = valueState.get().asScala.toSeq
    log.info(s"OnTimer: ${valueSeq.mkString(",")}")
    for(value <- valueSeq) {
      out.collect(s"$value")
    }
    valueState.clear()
  }
}