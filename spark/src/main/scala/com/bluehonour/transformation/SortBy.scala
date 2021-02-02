package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * This function sorts the input RDD's data and stores it in a new RDD.
 * The first parameter requires you to specify a function which  maps the input data into the key that you want to sortBy.
 * The second parameter (optional) specifies whether you want the data to be sorted in ascending or descending order.
 */
object SortBy extends SparkContext with App {
  val y = sc.parallelize(Array(5, 7, 1, 3, 2, 1))
  val result: Array[Int] = y.sortBy(c => c, true).collect
  println(result.mkString(","))
  //  1,1,2,3,5,7

  val result2: Array[Int] = y.sortBy(c => c, false).collect
  println(result2.mkString(","))
  //  7,5,3,2,1,1

  val z = sc.parallelize(Array(("H", 10), ("A", 26), ("Z", 1), ("L", 5)))
  val result3: Array[(String, Int)] = z.sortBy(c => c._1, true).collect
  println(result3.mkString(","))
  //  (A,26),(H,10),(L,5),(Z,1)

  val result4: Array[(String, Int)] = z.sortBy(c => c._2, true).collect
  println(result4.mkString(","))
  //  (Z,1),(L,5),(H,10),(A,26)
}
