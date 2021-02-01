package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Converts the RDD into a Scala array and returns it.
 * If you provide a standard map-function (i.e. f = T -> U) it will be applied before inserting the values into the result array.
 */
object Collect extends SparkContext with App {
  val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
  val res: Array[String] = c.collect
  println(res.mkString(","))
  // Gnu,Cat,Rat,Dog,Gnu,Rat

}
