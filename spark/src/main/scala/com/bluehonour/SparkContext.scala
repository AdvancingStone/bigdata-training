package com.bluehonour

import org.apache.spark
import org.apache.spark.sql.SparkSession

class SparkContext {
  val session: SparkSession = SparkSession.builder()
    .appName(this.getClass.getCanonicalName)
    .master("local[*]")
    .getOrCreate()
  val sc: spark.SparkContext = session.sparkContext
  sc.setLogLevel("ERROR")
}
