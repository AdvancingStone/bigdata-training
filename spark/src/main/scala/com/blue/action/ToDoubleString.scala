package com.blue.action

import com.blue.SparkContext

/**
 * Returns a string that contains debug information about the RDD and its dependencies.
 */
object ToDoubleString extends SparkContext with App {

  val a = sc.parallelize(1 to 9, 3)
  val b = sc.parallelize(1 to 3, 3)
  val c = a.subtract(b)
  val result: String = c.toDebugString
  println(result)
  //(3) MapPartitionsRDD[5] at subtract at ToDoubleString.scala:12 []
  // |  SubtractedRDD[4] at subtract at ToDoubleString.scala:12 []
  // +-(3) MapPartitionsRDD[2] at subtract at ToDoubleString.scala:12 []
  // |  |  ParallelCollectionRDD[0] at parallelize at ToDoubleString.scala:10 []
  // +-(3) MapPartitionsRDD[3] at subtract at ToDoubleString.scala:12 []
  //    |  ParallelCollectionRDD[1] at parallelize at ToDoubleString.scala:11 []
}
