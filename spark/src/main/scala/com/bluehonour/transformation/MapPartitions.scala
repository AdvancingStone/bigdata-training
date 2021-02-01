package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * This is a specialized map that is called only once for each partition.
 * The entire content of the respective partitions is available as a sequential stream of values via the input argument (Iterarator[T]).
 * The custom function must return yet another Iterator[U]. The combined result iterators are automatically converted into a new RDD.
 * Please note, that the tuples (3,4) and (6,7) are missing from the following result due to the partitioning we chose.
 */
object MapPartitions extends SparkContext with App {
  // example1
  val a = sc.parallelize(1 to 9, 1)
  def myfunc1[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next()
    while(iter.hasNext){
      val cur = iter.next()
      res ::= (pre, cur)
      pre = cur
    }
    res.iterator
  }

  val result1: Array[(Int, Int)] = a.mapPartitions(myfunc1).collect
  println(result1.mkString(","))
  //  (8,9),(7,8),(6,7),(5,6),(4,5),(3,4),(2,3),(1,2)

  //example2
  val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),1)
  def myfunc2(iter: Iterator[Int]): Iterator[Int] = {
    var res = List[Int]()
    while (iter.hasNext){
      val cur = iter.next()
      res = res ::: List.fill(2)(cur)
    }
    res.iterator
  }

  val result2: Array[Int] = b.mapPartitions(myfunc2).collect
  println(result2.mkString(","))
  //  1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10

}