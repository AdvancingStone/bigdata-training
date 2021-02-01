package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Evaluates a boolean function for each data item of the RDD and puts the items for which the function returned true into the resulting RDD.
 */
object Filter extends SparkContext with App {

  val a = sc.parallelize(1 to 10, 3)
  val b = a.filter(_ % 2 == 0)
  val result: Array[Int] = b.collect
  println(result.mkString(","))
  //  2,4,6,8,10
}
