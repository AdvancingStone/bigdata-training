package com.blue.action

import com.blue.SparkContext

/**
 * Works like reduce except reduces the elements of the RDD in a multi-level tree pattern
 */
object TreeReduce extends SparkContext with App {

  val z = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
  val result: Int = z.treeReduce(_ + _)
  println(result)

}
