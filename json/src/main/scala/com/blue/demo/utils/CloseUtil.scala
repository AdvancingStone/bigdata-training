package com.blue.demo.utils

object CloseUtil {

  /**
   * using函数有两个参数a: A、f: A => B，其中a是需要关闭的资源，f是一个输入为A输出为B的函数。
   * 可以利用using函数来重写数据库操作和文本操作
   */
  def using[A <: {def close(): Unit}, B](a: A)(f: A => B): B = {
    try f(a)
    finally {
      if (a != null) a.close()
    }
  }
}
