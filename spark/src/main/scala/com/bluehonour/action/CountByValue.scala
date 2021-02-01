package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Returns a map that contains all unique values of the RDD and their respective occurrence counts.
 * (Warning: This operation will finally aggregate the information in a single reducer.)
 */
object CountByValue extends SparkContext with App {
  val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
  val value: collection.Map[Int, Long] = b.countByValue
  println(value)
  //  Map(5 -> 1, 1 -> 6, 6 -> 1, 2 -> 3, 7 -> 1, 3 -> 1, 8 -> 1, 4 -> 2)
}
