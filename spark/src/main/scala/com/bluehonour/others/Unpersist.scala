package com.bluehonour.others

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Dematerializes the RDD (i.e. Erases all data items from hard-disk and memory).
 * However, the RDD object remains.
 * If it is referenced in a computation, Spark will regenerate it automatically using the stored dependency graph.
 */
object Unpersist extends SparkContext with App {
  val y = sc.parallelize(1 to 10, 10)
  val z: RDD[Int] = (y ++ y)
  val res1: Array[Int] = z.collect
  println(res1.mkString(","))
  //  1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10
  val res2: z.type = z.unpersist(true)
  println(res2)
  //  UnionRDD[1] at $plus$plus at Unpersist.scala:13
  println(z.collect.mkString(","))
  //  1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10
}
