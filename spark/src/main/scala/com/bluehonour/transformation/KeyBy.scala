package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Constructs two-component tuples (key-value pairs) by applying a function on each data item.
 * The result of the function becomes the key and the original data item becomes the value of the newly created tuples.
 */
object KeyBy extends SparkContext with App {

  val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  val b = a.keyBy(_.length)
  val result: Array[(Int, String)] = b.collect
  println(result.deep.mkString("\n"))
  //  (3,dog)
  //  (6,salmon)
  //  (6,salmon)
  //  (3,rat)
  //  (8,elephant)
}
