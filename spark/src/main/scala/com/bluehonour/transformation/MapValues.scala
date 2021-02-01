package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Takes the values of a RDD that consists of two-component tuples, and applies the provided function to transform each value.
 * Then, it forms new two-component tuples using the key and the transformed value and stores them in a new RDD.
 */
object MapValues extends SparkContext with App {

  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val b = a.map(x => (x.length, x))
  private val result: Array[(Int, String)] = b.mapValues("[" + _ + "]").collect
  println(result.mkString(","))
  //  (3,[dog]),(5,[tiger]),(4,[lion]),(3,[cat]),(7,[panther]),(5,[eagle])
}
