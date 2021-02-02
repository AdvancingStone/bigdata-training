package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Similar to zip. But provides more control over the zipping process
 */
object ZipPartitions extends SparkContext with App {

  val a = sc.parallelize(0 to 9, 3)
  val b = sc.parallelize(10 to 19, 3)
  val c = sc.parallelize(100 to 109, 3)

  def myfunc(iter1: Iterator[Int], iter2: Iterator[Int], iter3: Iterator[Int]): Iterator[String] = {
    var res = List[String]()
    while (iter1.hasNext && iter2.hasNext && iter3.hasNext) {
      val x = iter1.next() + " " + iter2.next() + " " + iter3.next()
      res ::= x
    }
    res.iterator
  }

  val result: Array[String] = a.zipPartitions(b, c)(myfunc).collect
  println(result.mkString(","))
  //  2 12 102,1 11 101,0 10 100,5 15 105,4 14 104,3 13 103,9 19 109,8 18 108,7 17 107,6 16 106
}
