package com.blue.action

import com.blue.SparkContext

/**
 * Aggregates the values of each partition.
 * The aggregation variable within each partition is initialized with zeroValue.
 */
object Fold extends SparkContext with App {

  val a = sc.parallelize(List(1, 2, 3), 3)
  val result: Int = a.fold(0)(_ + _)
  println(result) //6

}
