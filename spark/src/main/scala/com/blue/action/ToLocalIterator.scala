package com.blue.action

import com.blue.SparkContext

/**
 * Converts the RDD into a scala iterator at the master node.
 */
object ToLocalIterator extends SparkContext with App {

  val z = sc.parallelize(List(1,2,3,4,5,6), 2)
  val iter: Iterator[Int] = z.toLocalIterator
  println(iter.next())  // 1
  println(iter.next())  // 2
}
