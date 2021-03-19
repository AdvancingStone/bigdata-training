package com.bluehonour.sparksql

import com.bluehonour.SparkSession
import org.apache.spark.sql.DataFrame

object Demo1 extends App with SparkSession {

  val df = sparkSession.createDataFrame(Seq(
    ("L'OREAL PARIS ALL", "yes", "yes", "57161", "113653"),
    ("MAYBELLINE", "no", "no", "n/a", "n/a"),
    ("ULTRA DOUX", "N/A", "N/A", "20395", "27424")
  )).toDF("brandzone", "pc", "mob", "pc2", "mob2")

  df.show()

  val df1: DataFrame = df.selectExpr("*", "if(pc='yes', 30, 0) + if(mob='yes', 70, 0) as score", "nvl(trim(pc2)+trim(mob2), 0) as total")
  df1.show()

  val df2: DataFrame = df1.selectExpr("*",
    """
      |case when total>500000 then 100
      | when total>200000 then (total-200000)/300000*20+80
      | when total>50000 then (total-50000)/150000*20+60
      | else total/50000*60
      | end as score2
      |""".stripMargin)
  df2.show()

  val df3: DataFrame = df2.selectExpr("*", "pc2/mob2 as a", "total/score as b", "if(cast(pc2 as decimal)>cast(mob2 as decimal), pc2, mob2) as c")
  df3.show()

  df.selectExpr("*", "if(pc='yes', 30, 0) + if(mob='yes', 70, 0) as score", "nvl(trim(pc2)+trim(mob2), 0) as total")
      .selectExpr("*",
        """
          |case when total>500000 then 100
          | when total>200000 then (total-200000)/300000*20+80
          | when total>50000 then (total-50000)/150000*20+60
          | else total/50000*60
          | end as score2
          |""".stripMargin)
      .selectExpr("*", "pc2/mob2 as a", "total/score as b", "if(cast(pc2 as decimal)>cast(mob2 as decimal), pc2, mob2) as c")
      .show()

  df3.printSchema()
}
