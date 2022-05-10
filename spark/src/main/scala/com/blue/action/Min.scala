package com.blue.action

import com.blue.SparkContext

/**
 * Returns the smallest element in the RDD
 */
object Min extends SparkContext with App {

  val y = sc.parallelize(10 to 30)
  val result: Int = y.min
  println(result)
  // 10
}
