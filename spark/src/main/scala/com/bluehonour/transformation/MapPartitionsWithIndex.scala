package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Similar to mapPartitions, but takes two parameters.
 * The first parameter is the index of the partition and the second is an iterator through all the items within this partition.
 * The output is an iterator containing the list of items after applying whatever transformation the function encodes.
 */
object MapPartitionsWithIndex extends SparkContext with App {
  val x = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

  def myfunc(index: Int, iter: Iterator[Int]): Iterator[String] = {
    iter.map(x => s"index: $index and value: $x")
  }

  val result: Array[String] = x.mapPartitionsWithIndex(myfunc).collect()
  println(result.mkString(","))
  // index: 0 and value: 1,index: 0 and value: 2,index: 0 and value: 3,index: 1 and value: 4,index: 1 and value: 5,index: 1 and value: 6,index: 2 and value: 7,index: 2 and value: 8,index: 2 and value: 9,index: 2 and value: 10
}
