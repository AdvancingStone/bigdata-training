package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Utilizes the implicit ordering of $T$ to determine the top $k$ values and returns them as an array.
 */
object Top extends SparkContext with App {

  val c = sc.parallelize(Array(6, 9, 4, 7, 5, 8), 2)
  val result: Array[Int] = c.top(2)
  println(result.mkString(","))
  //  9,8
}
