package com.blue.demo

import com.alibaba.fastjson.JSON
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.{JedisPool, Tuple}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

object SparkOnHiveLocalDemo {

  case class Location(uuid: String, msr_id: String, country: String, city: String, district: String, time: Long)

  var redisMin: Long = _
  val redisMax: Long = System.currentTimeMillis()
  val jedisKey = "device:public_track"

  val host = "master"
  val port = 3306
  val database = "todays-tableau"
  val table = "redis_index_info"
  val jdbcUrl = s"jdbc:mysql://$host:$port/$database"

  def main(args: Array[String]): Unit = {

    Class.forName("com.mysql.jdbc.Driver")
    val conn: Connection = DriverManager.getConnection(jdbcUrl, "root", "liushuai")
    val sql = s"select max(timestamps) as max_timestamp from `${database}`.${table}"
    val ps: PreparedStatement = conn.prepareStatement(sql)
    val result = ps.executeQuery()
    while (result.next()) {
      val max_timestamp = result.getString("max_timestamp")
      redisMin = max_timestamp.toLong
      println(s"当前redis的时间戳为: ${max_timestamp}")
    }

    val session: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[4]")
      // 连接hive地址，如果复制hive-site.xml到resources下，则不需要此配置,hive-site是生产的，使用本地的要用自己本地的配置覆盖
      .config("hive.metastore.uris", "thrift://master:9083")
      .enableHiveSupport()
      .getOrCreate()
    val sc: spark.SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    val pool = new JedisPool("master", 6379)
    val jedis = pool.getResource
    jedis.auth("liushuai")

    val count = jedis.zcount(jedisKey, redisMin, redisMax)
    println(s"redis的新数据量为: ${count}")
    var list = List[Location]()

    val dim_df: DataFrame = session.sql("select province, city from dv_report.dim_province_city group by province, city").cache()

    val iter: util.Iterator[String] = jedis.zrangeByScore(jedisKey, redisMin, redisMax).iterator()
    while (iter.hasNext) {
      val jsonStr = iter.next()
      println(jsonStr)
      val jsonObj = JSON.parseObject(jsonStr)
      val uuid = jsonObj.getString("id")
      val msr_id = jsonObj.getString("customerId")
      val city = jsonObj.getString("cityName")
      val district = jsonObj.getString("district")
      val country = "中国"
      val time = jedis.zscore(jedisKey, jsonStr).toLong.toString.substring(0, 10).toLong
      val location = Location(uuid, msr_id, country, city, district, time)
      list = list :+ location
      if (list.size >= 1000) { //list超过一千条记录写一次hive
        println(list)
        writeHive(session, sc, dim_df, list)
        list = List[Location]()
      }
    }
    writeHive(session, sc, dim_df, list)

    val insertSql = s"insert into `${database}`.${table} values(?,?)"
    val instertPreparedStatement: PreparedStatement = conn.prepareStatement(insertSql)
    instertPreparedStatement.setString(1, s"${redisMax}")
    instertPreparedStatement.setString(2, s"${count}")
    instertPreparedStatement.executeUpdate()
    println(s"redis index info (timestamps, number)->(${redisMax}, ${count}) has been insert into mysql...")

    dim_df.unpersist()
    sc.stop()
    session.close()

  }


  def writeHive(session: SparkSession, sc: spark.SparkContext, dim_df: DataFrame, list: List[Location]): Unit = {
    val rdd = sc.parallelize(list)

    import session.implicits._
    val resultDf = rdd.toDF().join(dim_df, Seq("city")).selectExpr(exprs = "uuid", "msr_id", "country", "province", "city", "district", "time")
    resultDf.show(10)
    resultDf.createOrReplaceTempView("tmpTable")
    session.sql(
      """
        |insert into table dv_report.dw_ams_cust_location_tmp
        |select * from tmpTable
        |""".stripMargin)
  }


}
