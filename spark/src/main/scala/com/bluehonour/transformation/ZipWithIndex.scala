package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Zips the elements of the RDD with its element indexes.
 * The indexes start from 0.
 * If the RDD is spread across multiple partitions then a spark Job is started to perform this operation.
 */
object ZipWithIndex extends SparkContext with App {
  val z = sc.parallelize(Array("A", "B", "C", "D"))
  private val res1: Array[(String, Long)] = z.zipWithIndex.collect
  println(res1.mkString(","))
  //  (A,0),(B,1),(C,2),(D,3)

  val x = sc.parallelize(100 to 120, 5)
  val res2: Array[(Int, Long)] = x.zipWithIndex.collect
  println(res2.mkString(","))
  //  (100,0),(101,1),(102,2),(103,3),(104,4),(105,5),(106,6),(107,7),(108,8),(109,9),
  //  (110,10),(111,11),(112,12),(113,13),(114,14),(115,15),(116,16),(117,17),(118,18),(119,19),(120,20)
}
