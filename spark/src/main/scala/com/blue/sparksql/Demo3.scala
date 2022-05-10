package com.blue.sparksql

import com.blue.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, Row}

object Demo3 extends SparkSession with App {

  val seq: Seq[(String, String)] = Seq(("LANCÔME", "17056246"), ("YVES SAINT LAURENT", "11361876"), ("KIEHL'S", "5292238"), ("HELENA RUBINSTEIN", "226940"),
    ("L'OREAL PARIS ALL", "NA"), ("MAYBELLINE", "1035558"), ("3CE", "NA"), ("GIORGIO ARMANI", "6560577"),
    ("SHU UEMURA", "3130158"), ("SAINT-GERVAIS MONT-BLANC", "28788"), ("YUE SAI", "324624"),
    ("SKINCEUTICALS", "157540"), ("VICHY", "NA"), ("BIOTHERM", "1886304"), ("KERASTASE", "440258"),
    ("MY BEAUTY BOX", "NA"), ("ATELIER COLOGNE", "77670"), ("L'OREAL PROFESSIONNEL", "69354"), ("CERAVE", "NA"),
    ("MAGIC", "NA"), ("L'OREAL PARIS MEX", "NA"), ("L'OREAL PARIS HAIR", "NA"), ("L'OREAL CHINA", "NA"), ("LA ROCHE-POSAY", "NA"))

  val df: DataFrame = sparkSession.createDataFrame(seq)
    .toDF("brand", "value")

  val df1: DataFrame = df.selectExpr("*")

  val df2: DataFrame = df.selectExpr("avg(value) as avg", "stddev(value) as stddev").selectExpr("*")

  df1.show()
  df2.show()

  val rdd1: RDD[Row] = df1.rdd
  val rdd2: RDD[Row] = df2.rdd

  val encoder = Encoders.tuple(
    RowEncoder(
      StructType(Seq(
        StructField("brand", StringType),
        StructField("value", StringType),
        StructField("a", StringType)
      ))

    )
    , RowEncoder(
      StructType(Seq(
        StructField("avg", StringType),
        StructField("stddev", StringType),
        StructField("a", StringType)
      )))
  )
  val rdd: RDD[(Row, Row)] = rdd1.cartesian(rdd2)
  println("aaaaaaaaaaaaa")
  rdd.collect.foreach(println)


}
