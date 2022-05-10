package com.blue.others

import com.blue.SparkContext

/**
 * Coalesces the associated data into a given number of partitions.
 * repartition(numPartitions) is simply an abbreviation for coalesce(numPartitions, shuffle = true).
 */
object Coalesce extends SparkContext with App {

  val y = sc.parallelize(1 to 10, 10)
  val z = y.coalesce(2, false)
  val length: Int = z.partitions.length
  println(length)
  //2
}
