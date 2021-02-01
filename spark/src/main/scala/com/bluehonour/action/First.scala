package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Looks for the very first data item of the RDD and returns it.
 */
object First extends SparkContext with App {

  val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
  val result: String = c.first
  println(result)
  //  Gnu
}
