package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Computes the same thing as aggregate, except it aggregates the elements of the RDD in a multi-level tree pattern.
 * Another difference is that it does not use the initial value for the second reduce function (combOp).
 * By default a tree of depth 2 is used, but this can be changed via the depth parameter.
 */
object TreeAggregate extends SparkContext with App {
  val z = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)

  // lets first print out the contents if the RDD with partition labels
  def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
    iter.map(x => s"[partID: $index, value: $x]")
  }

  val res1: Array[String] = z.mapPartitionsWithIndex(myfunc).collect
  println(res1.mkString(","))
  //  [partID: 0, value: 1],[partID: 0, value: 2],[partID: 0, value: 3],[partID: 1, value: 4],[partID: 1, value: 5],[partID: 1, value: 6]

  // Note unlike normal aggregate. Tree aggregate does not apply the initial value for the second reduce
  // This example returns 11 since the initial value is 5
  // reduce of partition 0 will be max(5, 1, 2, 3) = 5
  // reduce of partition 1 will be max(5, 4, 5, 6) = 6
  // final reduce across partitions will be 5 + 6 = 11
  // note the final reduce does not include the initial value
  val res2: Int = z.treeAggregate(5)(math.max(_, _), _ + _)
  println(res2)
  //  11


}
