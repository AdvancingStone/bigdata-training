package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Performs the well known standard set subtraction operation: A - B
 */
object Subtract extends SparkContext with App {
  val a = sc.parallelize(1 to 9, 3)
  val b = sc.parallelize(1 to 3, 3)
  val c: RDD[Int] = a.subtract(b)
  val result: Array[Int] = c.collect
  println(result.mkString(","))
  //  6,9,4,7,5,8
}
