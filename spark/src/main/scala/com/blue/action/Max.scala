package com.blue.action

import com.blue.SparkContext

/**
 * Returns the largest element in the RDD
 */
object Max extends SparkContext with App {
  val y = sc.parallelize(10 to 30)
  val result: Int = y.max
  println(result)
  // 30
}
