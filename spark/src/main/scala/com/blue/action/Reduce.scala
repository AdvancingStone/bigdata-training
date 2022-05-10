package com.blue.action

import com.blue.SparkContext

/**
 * This function provides the well-known reduce functionality in Spark.
 * Please note that any function if you provide, should be commutative in order to generate reproducible results.
 */
object Reduce extends SparkContext with App {
  val a = sc.parallelize(1 to 100, 3)
  private val result: Int = a.reduce(_ + _)
  println(result) //5050

}
