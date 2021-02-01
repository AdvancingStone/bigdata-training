package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Executes an parameterless function for each partition.
 * Access to the data items contained in the partition is provided via the iterator argument.
 */
object ForeachPartition extends SparkContext with App {
  val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
  b.foreachPartition(x => println(x.reduce(_ + _)))
  //  40
  //  15
}
