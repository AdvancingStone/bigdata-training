package com.bluehonour.transformation

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * This function sorts the input RDD's data and stores it in a new RDD.
 * The output RDD is a shuffled RDD because it stores data that is output by a reducer which has been shuffled.
 * The implementation of this function is actually very clever.
 * First, it uses a range partitioner to partition the data in ranges within the shuffled RDD.
 * Then it sorts these ranges individually with mapPartitions using standard sort mechanisms.
 */
object SortByKey extends SparkContext with App {

  val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
  val b = sc.parallelize(1 to a.count.toInt, 2)
  val c: RDD[(String, Int)] = a.zip(b)
  val result: Array[(String, Int)] = c.sortByKey(true).collect
  println(result.mkString(","))
  //  (ant,5),(cat,2),(dog,1),(gnu,4),(owl,3)

  val result2: Array[(String, Int)] = c.sortByKey(false).collect
  println(result2.mkString(","))
  //  (owl,3),(gnu,4),(dog,1),(cat,2),(ant,5)

  val x = sc.parallelize(1 to 100, 5)
  val y: RDD[(Int, Int)] = x.cartesian(x)
  val z: RDD[(Int, Int)] = sc.parallelize(y.takeSample(true, 5, 13), 2)
  val result3: Array[(Int, Int)] = z.sortByKey(false).collect()
  println(result3.mkString(","))
  //  (79,99),(75,78),(27,42),(12,30),(6,73)
}
