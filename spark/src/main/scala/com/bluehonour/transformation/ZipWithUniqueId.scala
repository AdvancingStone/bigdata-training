package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * This is different from zipWithIndex since just gives a unique id to each data element but the ids may not match the index number of the data element.
 * This operation does not start a spark job even if the RDD is spread across multiple partitions.
 */
object ZipWithUniqueId extends SparkContext with App {

  val z = sc.parallelize(1 to 20, 5)
  val r = z.zipWithUniqueId
  val res: Array[(Int, Long)] = r.collect
  println(res.mkString(","))
  //  (1,0),(2,5),(3,10),(4,15),
  //  (5,1),(6,6),(7,11),(8,16),
  //  (9,2),(10,7),(11,12),(12,17),
  //  (13,3),(14,8),(15,13),(16,18),
  //  (17,4),(18,9),(19,14),(20,19)
}
