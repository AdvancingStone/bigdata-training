package com.blue.transformation

import com.blue.SparkContext

/**
 * Performs the full outer join between two paired RDDs.
 */
object FullOuterJoin extends SparkContext with App {
  val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12)))
  val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))
  val result: Array[(String, (Option[Int], Option[Int]))] = pairRDD1.fullOuterJoin(pairRDD2).collect
  println(result.mkString(","))
  //  (book,(Some(4),None)),
  //  (mouse,(None,Some(4))),
  //  (cup,(None,Some(5))),
  //  (cat,(Some(2),Some(2))),
  //  (cat,(Some(2),Some(12))),
  //  (cat,(Some(5),Some(2))),
  //  (cat,(Some(5),Some(12))),
  //  (cat,(Some(12),Some(2))),
  //  (cat,(Some(12),Some(12)))

}
