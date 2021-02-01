package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * These functions take an RDD of doubles and create a histogram with either even spacing(the number of buckets equals to bucketCount)
 * or arbitrary spacing based on  custom bucket boundaries supplied by the user via an array of double values.
 * The result type of both variants is slightly different, the first function will return a tuple consisting of two arrays.
 * The first array contains the computed bucket boundary values and the second array contains the corresponding count of values (i.e. the histogram).
 * The second variant of the function will just return the histogram as an array of integers.
 *
 * 这些函数接收一个双数的 RDD，并创建一个间距均匀的直方图（桶的数量等于 bucketCount）
 * 或基于用户通过双数组提供的自定义桶边界的任意间距的直方图。
 * 两个变体的结果类型略有不同，第一个函数将返回一个由两个数组组成的元组。
 * 第一个数组包含计算出的bucket边界值，第二个数组包含相应的数值计数（即直方图）。
 * 第二种函数的变体只是将直方图作为一个整数组返回。
 *
 * Listing Variants
 *  def histogram(bucketCount: Int): Pair[Array[Double], Array[Long]]
 *  def histogram(buckets: Array[Double], evenBuckets: Boolean = false): Array[Long]
 */
object Histogram extends SparkContext with App {

  //Example with even spacing
  val a = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0), 3)
  val result1: (Array[Double], Array[Long]) = a.histogram(5)
  println(result1._1.mkString(","))
  //  1.1,2.68,4.26,5.84,7.42,9.0
  println(result1._2.mkString(","))
  //  5,0,0,1,4

  val b = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5), 3)
  val result2: (Array[Double], Array[Long]) = b.histogram(6)
  println(result2._1.mkString(","))
  //  1.0,2.5,4.0,5.5,7.0,8.5,10.0
  println(result2._2.mkString(","))
  //  1.0,2.5,4.0,5.5,7.0,8.5,10.0


  //Example with custom spacing

  val c = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0), 3)
  val result3: Array[Long] = c.histogram(Array(0.0, 3.0, 8.0))
  println(result3.mkString(","))
  //  5,3

  val d = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5), 3)
  val result4: Array[Long] = d.histogram(Array(0.0, 5.0, 10.0))
  println(result4.mkString(","))
  //  6,9

  val result5: Array[Long] = d.histogram(Array(0.0, 5.0, 10.0, 15.0))
  println(result5.mkString(","))
  //  6,8,1
}
