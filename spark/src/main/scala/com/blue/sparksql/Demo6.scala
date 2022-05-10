package com.blue.sparksql

import com.blue.SparkSession
import org.apache.spark.sql.DataFrame

object Demo6 extends App with SparkSession {

  val df = sparkSession.createDataFrame(Seq(
    ("L'OREAL PARIS ALL", "yes", "yes", "57161", "113653"),
    ("MAYBELLINE", "no", "no", "22", "3"),
    ("ULTRA DOUX", "N/A", "N/A", "123", "321"),
    ("ULTRA DOUX", "N/A", "N/A", "n/a", "123"),
    ("MAYBELLINE", "no", "no", "11", "11"),
    ("MAYBELLINE", "", " ", "", "11")
  )).toDF("brand", "pc", "mob", "pc2", "mob2")

  val df2: DataFrame = df.selectExpr("brand", "cast(pc2 as decimal) as pc2")
  df2.show()

  df2.where("pc2 is not null").show

  df.selectExpr("pc2 * 0.2 + mob2 * 0.7 as aa",
    "if(upper(pc)='YES', 30, 0) + if(mob='YES', 70, 0) as brand_zone_score"
  ).show()


}
