package com.bluehonour.sparksql

import com.bluehonour.SparkSession
import org.apache.spark.sql.DataFrame

object Demo5 extends App with SparkSession {

  val df = sparkSession.createDataFrame(Seq(
    ("L'OREAL PARIS ALL", "yes", "yes", "57161", "113653"),
    ("MAYBELLINE", "no", "no", "22", "3"),
    ("ULTRA DOUX", "N/A", "N/A", "123", "321"),
    ("ULTRA DOUX", "N/A", "N/A", "n/a", "123"),
    ("MAYBELLINE", "no", "no", "11", "11")
  )).toDF("brand", "pc", "mob", "pc2", "mob2")

  val df2: DataFrame = df.selectExpr("brand", "cast(pc2 as decimal) as pc2")
  df2.printSchema()
  df2.show()
  val df3 = df2
    .groupBy("brand")
    .max("pc2").alias("pc2_max")

  df3.printSchema()
  df3.show()

  import org.apache.spark.sql.functions._

  df2.groupBy("brand")
    .agg(
      max("pc2").alias("max_pc2"),
      count("*").as("count")
    ).show()

}
