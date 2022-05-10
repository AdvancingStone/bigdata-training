package com.blue.transformation

import com.blue.SparkContext

/**
 * Joins two RDDs by combining the i-th of either partition with each other.
 * The resulting RDD will consist of two-component tuples which are interpreted as key-value pairs by the methods provided by the PairRDDFunctions extension.
 */
object Zip extends SparkContext with App {

  val a = sc.parallelize(1 to 10, 3)
  val b = sc.parallelize(101 to 110, 3)
  val res: Array[(Int, Int)] = a.zip(b).collect
  println(res.mkString(","))
  //  (1,101),(2,102),(3,103),(4,104),(5,105),(6,106),(7,107),(8,108),(9,109),(10,110)
}
