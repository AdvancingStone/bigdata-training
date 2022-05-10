package com.blue

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

trait SparkSession {
  val sparkSession: sql.SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local[*]")
    .getOrCreate()

}
