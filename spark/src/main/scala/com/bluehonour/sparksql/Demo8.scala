package com.bluehonour.sparksql

import com.bluehonour.SparkSession

object Demo8 extends App with SparkSession {
  val seq = Seq(
    ("aaa", "1", "0.6667", "0.898", "0.8231", "0.225", "0.2"),
    ("aaa", "2", "0.2", "0.898", "0.8231", "0.225", "0.2"),
    ("aaa", "2", "", "0.898", "0.8231", "0.225", "0.2")
  )
  val df = sparkSession.createDataFrame(
    seq
  ).toDF("brand",
    "brand_keywords_pc",
    "brand_keywords_mob",
    "product_keywords_pc",
    "product_keywords_mob",
    "generic_keywords_pc",
    "generic_keywords_mob")

  df.selectExpr(exprs =
//    "(brand_keywords_pc * 0.25 + brand_keywords_mob * 0.75) * 100 as brand_keywords_ranking_score",
//    "(product_keywords_pc * 0.25 + product_keywords_mob * 0.75) * 100 as product_keywords_ranking_score",
//    "(generic_keywords_pc * 0.25 + generic_keywords_mob * 0.75) * 100 as generic_keywords_ranking_score",
    "max(brand_keywords_pc) as max_brand_keywords_pc",
    "max(brand_keywords_mob) as max_brand_keywords_mob"

  ).show()

}
