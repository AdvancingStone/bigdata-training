package com.bluehonour.transformation

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Returns the elements in the two RDDs which are the same.
 */
object Intersection extends SparkContext with App {
  val x = sc.parallelize(1 to 20)
  val y = sc.parallelize(10 to 30)
  val z: RDD[Int] = x.intersection(y)
  val result: Array[Int] = z.collect
  println(result.mkString(","))
  //  16,17,18,10,19,11,20,12,13,14,15
}
