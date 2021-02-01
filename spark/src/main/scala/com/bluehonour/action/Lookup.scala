package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Scans the RDD for all keys that match the provided value and returns their values as a Scala sequence.
 */
object Lookup extends SparkContext with App {

  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val b = a.map(x => (x.length, x))
  private val result: Seq[String] = b.lookup(5)
  println(result)
  //  WrappedArray(tiger, eagle)

}
