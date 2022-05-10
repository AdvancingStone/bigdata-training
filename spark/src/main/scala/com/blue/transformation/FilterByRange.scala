package com.blue.transformation

import com.blue.SparkContext

/**
 * Returns an RDD containing only the items in the key range specified.
 */
object FilterByRange extends SparkContext with App {

  val randRDD = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
  val result: Array[(Int, String)] = randRDD.filterByRange(1, 3).collect
  println(result.mkString(","))
  //  (1,screen),(2,cat),(3,book)
}
