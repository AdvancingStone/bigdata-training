package com.bluehonour.sparksql

import com.bluehonour.SparkSession
import org.apache.spark.sql.DataFrame

object Demo2 extends SparkSession with App {

  val seq: Seq[(String, String)] = Seq(("LANCÔME", "17056246"), ("YVES SAINT LAURENT", "11361876"), ("KIEHL'S", "5292238"), ("HELENA RUBINSTEIN", "226940"),
    ("L'OREAL PARIS ALL", "NA"), ("MAYBELLINE", "1035558"), ("3CE", "NA"), ("GIORGIO ARMANI", "6560577"),
    ("SHU UEMURA", "3130158"), ("SAINT-GERVAIS MONT-BLANC", "28788"), ("YUE SAI", "324624"),
    ("SKINCEUTICALS", "157540"), ("VICHY", "NA"), ("BIOTHERM", "1886304"), ("KERASTASE", "440258"),
    ("MY BEAUTY BOX", "NA"), ("ATELIER COLOGNE", "77670"), ("L'OREAL PROFESSIONNEL", "69354"), ("CERAVE", "NA"),
    ("MAGIC", "NA"), ("L'OREAL PARIS MEX", "NA"), ("L'OREAL PARIS HAIR", "NA"), ("L'OREAL CHINA", "NA"), ("LA ROCHE-POSAY", "NA"))

  val df: DataFrame = sparkSession.createDataFrame(seq)
    .toDF("brand", "value")

  val df1: DataFrame = df.selectExpr("*", "1 as a")

  val df2: DataFrame = df.selectExpr("avg(value) as avg", "stddev(value) as stddev").selectExpr("*", "1 as a")

  df1.show()
  df2.show()

  val df3 = df1.join(df2, Seq("a"), "full")
    .selectExpr(exprs =
      "a",
      "brand",
      "avg",
      "stddev",
      "avg * stddev as avg_stddev"
    )
  println("df3====================")
  df3.show()

  val df4: DataFrame = df1.crossJoin(df2)
  println("df4====================")
  df4.show()

  df4.join(df3,Seq("brand")).join(df1,Seq("a")).show()

}













