package com.bluehonour.utils

import java.io.{ByteArrayOutputStream, PrintStream}

import org.slf4j.Logger

object ExceptionUtil {

  /**
   * 打印异常error日志，包含异常堆栈信息
   */
  def error(log: Logger, exception: Exception): Unit = {
    log.error(getStackTraceStr(exception))
  }

  /**
   * 获取异常堆栈信息
   */
  def getStackTraceStr(exception: Exception): String = {
    val outputStream = new ByteArrayOutputStream()
    exception.printStackTrace(new PrintStream(outputStream))
    outputStream.toString
  }
}
