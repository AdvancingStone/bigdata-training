package com.bluehonour.action

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Computes the approximate number of distinct values.
 * For large RDDs which are spread across many nodes, this function may execute faster than other counting methods.
 * The parameter relativeSD controls the accuracy of the computation.
 */
object CountApproxDistinct extends SparkContext with App {

  val a: RDD[Int] = sc.parallelize(1 to 1000, 2)
  val b: RDD[Int] = a ++ a ++ a ++ a ++ a
  val arr: Array[Int] = b.collect()
  println(arr.mkString(","))  //5个 1 to 1000

  val result1: Long = b.countApproxDistinct(0.1)
  val result2: Long = b.countApproxDistinct(0.05)
  val result3: Long = b.countApproxDistinct(0.01)
  val result4: Long = b.countApproxDistinct(0.001)
  println(result1) //1194
  println(result2) //1006
  println(result3) //1001
  println(result4) //1000

}
