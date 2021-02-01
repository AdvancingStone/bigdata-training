package com.bluehonour.transformation

import org.apache.spark.rdd.RDD

/**
 * repartition(numPartitions) is simply an abbreviation for coalesce(numPartitions, shuffle = true).
 */
object Repartition extends SparkContext with App {
  val x: RDD[Int] = sc.parallelize(1 to 10, 10)
  val y: RDD[Int] = x.repartition(3)
  println(y.partitions.length)
  // 3
}
