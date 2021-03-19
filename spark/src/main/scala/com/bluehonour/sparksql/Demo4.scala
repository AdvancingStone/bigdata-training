package com.bluehonour.sparksql

import com.bluehonour.SparkSession
import org.apache.spark.sql.DataFrame

object Demo4 extends App with SparkSession {

  val df = sparkSession.createDataFrame(Seq(
    ("L'OREAL PARIS ALL", "yes", "yes", "57161", "113653"),
    ("MAYBELLINE", "no", "no", "n/a", "n/a"),
    ("ULTRA DOUX", "N/A", "N/A", "20395", "27424")
  )).toDF("brand", "pc", "mob", "pc2", "mob2")

  val df2 = sparkSession.createDataFrame(Seq(
    ("L'OREAL PARIS ALL", "2"),
    ("MAYBELLINE", "1"),
    ("ULTRA DOUX", "3")
  )).toDF("brand", "pc")

  val df3 = sparkSession.createDataFrame(Seq(
    ("L'OREAL PARIS ALL", "2"),
    ("MAYBELLINE", "1"),
    ("ULTRA DOUX", "3"),
    ("aaa", "3")
  )).toDF("brand", "mob")

//  val resultDF: DataFrame = df.join(df2, Seq("brand"))
//  resultDF
  df.join(df2, Seq("brand"))
    .join(df3, Seq("brand"), "full")
    .selectExpr("*")
    .show()

}
