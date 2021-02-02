package com.bluehonour.transformation

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Performs an right outer join using two key-value RDDs.
 * Please note that the keys must be generally comparable to make this work correctly.
 */
object RightOuterJoin extends SparkContext with App {

  val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  val b: RDD[(Int, String)] = a.keyBy(_.length)
  println(b.collect.mkString(","))
  //  (3,dog),(6,salmon),(6,salmon),(3,rat),(8,elephant)

  val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
  val d: RDD[(Int, String)] = c.keyBy(_.length)
  println(d.collect.mkString(","))
  //  (3,dog),(3,cat),(3,gnu),(6,salmon),(6,rabbit),(6,turkey),(4,wolf),(4,bear),(3,bee)

  val result: Array[(Int, (Option[String], String))] = b.rightOuterJoin(d).collect
  println(result.mkString(","))
  //  (6,(Some(salmon),salmon)),
  //  (6,(Some(salmon),rabbit)),
  //  (6,(Some(salmon),turkey)),
  //  (6,(Some(salmon),salmon)),
  //  (6,(Some(salmon),rabbit)),
  //  (6,(Some(salmon),turkey)),
  //  (3,(Some(dog),dog)),
  //  (3,(Some(dog),cat)),
  //  (3,(Some(dog),gnu)),
  //  (3,(Some(dog),bee)),
  //  (3,(Some(rat),dog)),
  //  (3,(Some(rat),cat)),
  //  (3,(Some(rat),gnu)),
  //  (3,(Some(rat),bee)),
  //  (4,(None,wolf)),
  //  (4,(None,bear))
}
