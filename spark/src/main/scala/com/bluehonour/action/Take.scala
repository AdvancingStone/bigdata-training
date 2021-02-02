package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Extracts the first n items of the RDD and returns them as an array.
 * (Note: This sounds very easy, but it is actually quite a tricky problem for the implementors of Spark because the items in question can be in many different partitions.)
 */
object Take extends SparkContext with App {

  val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
  private val result: Array[String] = b.take(2)
  println(result.mkString(","))
  //  dog,cat
}
