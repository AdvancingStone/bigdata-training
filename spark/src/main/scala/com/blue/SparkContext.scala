package com.blue

import org.apache.spark

trait SparkContext extends SparkSession {
  val sc: spark.SparkContext = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
}
