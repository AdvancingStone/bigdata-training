package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Performs the standard set operation: A union B
 * Listing Variants
 * -  def ++(other: RDD[T]): RDD[T]
 * -  def union(other: RDD[T]): RDD[T]
 */
object Union extends SparkContext with App {
  val a = sc.parallelize(1 to 3, 1)
  val b = sc.parallelize(5 to 7, 1)
  val res1: Array[Int] = (a ++ b).collect
  val res2: Array[Int] = a.union(b).collect
  println(res1.mkString(","))
  println(res2.mkString(","))
  //  1,2,3,5,6,7
  //  1,2,3,5,6,7
}
