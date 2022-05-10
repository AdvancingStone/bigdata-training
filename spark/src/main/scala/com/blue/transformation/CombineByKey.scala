package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Very efficient implementation that combines the values of a RDD consisting of two-component tuples by applying multiple aggregators one after another.
 */
object CombineByKey extends SparkContext with App {

  val a = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
  val b = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
  val c = b.zip(a)
  val resultRdd: RDD[(Int, List[String])] = c.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
  val collect: Array[(Int, List[String])] = resultRdd.collect
  println(collect.mkString(","))
  //  (1,List(cat, dog, turkey)),(2,List(gnu, rabbit, salmon, bee, bear, wolf))

}
