package com.blue.transformation

import com.blue.SparkContext

/**
 * Similar to countApproxDistinct, but computes the approximate number of distinct values for each distinct key.
 * Hence, the RDD must consist of two-component tuples.
 * For large RDDs which are spread across many nodes, this function may execute faster than other counting methods.
 * The parameter relativeSD controls the accuracy of the computation.
 */
object CountApproxDistinctByKey extends SparkContext with App {
  val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
  val b = sc.parallelize(a.takeSample(true, 10000, 0), 20)
  val c = sc.parallelize(1 to b.count().toInt, 20)
  val d = b.zip(c)
  val result1: Array[(String, Long)] = d.countApproxDistinctByKey(0.1).collect
  val result2: Array[(String, Long)] = d.countApproxDistinctByKey(0.01).collect
  val result3: Array[(String, Long)] = d.countApproxDistinctByKey(0.001).collect

  println(result1.mkString(","))
  println(result2.mkString(","))
  println(result2.mkString(","))
  //  (Rat,2140),(Cat,2267),(Dog,2368),(Gnu,2488)
  //  (Rat,2505),(Cat,2480),(Dog,2562),(Gnu,2437)
  //  (Rat,2505),(Cat,2480),(Dog,2562),(Gnu,2437)

}
