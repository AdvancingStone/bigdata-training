package com.blue.action

import com.blue.SparkContext

/**
 * Returns the number of items stored within a RDD.
 */
object Count extends SparkContext with App {

  val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
  val count: Long = c.count
  println(count)  //  4

}
