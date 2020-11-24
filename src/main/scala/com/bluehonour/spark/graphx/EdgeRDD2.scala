package com.bluehonour.spark.graphx

import org.apache.spark.graphx.{Edge, EdgeRDD}
import org.apache.spark.sql.SparkSession

object EdgeRDD2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getCanonicalName}").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    EdgeRDD.fromEdges(edgeArray)
  }

}
