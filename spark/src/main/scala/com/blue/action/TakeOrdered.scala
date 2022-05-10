package com.blue.action

import com.blue.SparkContext

/**
 * Orders the data items of the RDD using their inherent implicit ordering function and returns the first n items as an array.
 */
object TakeOrdered extends SparkContext with App {

  val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
  val result: Array[String] = b.takeOrdered(2)
  println(result.mkString(","))
  //  ape,cat
}
