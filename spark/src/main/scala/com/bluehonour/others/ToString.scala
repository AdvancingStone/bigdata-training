package com.bluehonour.others

import com.bluehonour.SparkContext

/**
 * Assembles a human-readable textual description of the RDD.
 */
object ToString extends SparkContext with App {

  val z = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
  val res1: String = z.toString
  println(res1)
  //  ParallelCollectionRDD[0] at parallelize at ToString.scala:10

  val randRDD = sc.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
  val sortedRDD = randRDD.sortByKey()
  val res2: String = sortedRDD.toString
  println(res2)
  //  ShuffledRDD[4] at sortByKey at ToString.scala:16

}
