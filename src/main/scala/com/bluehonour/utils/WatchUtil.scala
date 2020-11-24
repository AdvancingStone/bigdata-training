package com.bluehonour.utils

import java.util.concurrent.TimeUnit

/**
 * 统计工具类
 */
object WatchUtil {

  /**
   * 执行时间统计
   *
   * @param exec     要执行的函数
   * @param timeUnit 统计时间单位
   */
  def timeUseWatch(exec: () => Unit, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Long = {
    val startTime = System.currentTimeMillis()
    exec()
    val endTime = System.currentTimeMillis()
    timeUnit.convert(endTime - startTime, TimeUnit.MILLISECONDS)
  }

}
