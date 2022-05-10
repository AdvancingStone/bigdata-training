package com.blue.sparksql

import com.blue.SparkSession

object Demo7 extends App with SparkSession {

  val seq = Seq(
    ("LANCÔME", 50),
    ("YVES SAINT LAURENT", 88),
    ("KIEHL'S", 25),
    ("HELENA RUBINSTEIN", 25),
    ("L'OREAL PARIS ALL", 88),
    ("MAYBELLINE", 88),
    ("3CE", 0),
    ("GIORGIO ARMANI", 25),
    ("SHU UEMURA", 25),
    ("SAINT-GERVAIS MONT-BLANC", 125),
    ("LA ROCHE-POSAY", 25),
    ("YUE SAI", 25),
    ("SKINCEUTICALS", 0),
    ("VICHY", 75),
    ("BIOTHERM", 25),
    ("KERASTASE", 0),
    ("MY BEAUTY BOX", 163),
    ("ATELIER COLOGNE", 25),
    ("L'OREAL PROFESSIONNEL", 0),
    ("CERAVE", 0),
    ("MAGIC", 0),
    ("L'OREAL PARIS MEX", 0),
    ("L'OREAL PARIS HAIR", 0),
    ("L'OREAL CHINA", 0)
  )
  val df = sparkSession.createDataFrame(
    seq
  ).toDF("brand", "age")


  df.selectExpr(exprs =
    "avg(age)","stddev(age)"
  ).show()
}
