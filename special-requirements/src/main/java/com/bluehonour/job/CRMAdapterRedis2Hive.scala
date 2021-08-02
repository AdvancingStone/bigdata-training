package com.bluehonour.job

import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.{Jedis, JedisPool}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

object CRMAdapterRedis2Hive {

  case class Location(uuid: String, msr_id: String, country: String, city: String, district: String, time: Long)

  var redisMin: Long = _
  val redisMax: Long = System.currentTimeMillis()
  val jedisKey = "device:public_track"

  val database = "todays-tableau"
  val table = "redis_index_info"
  val jdbcUrl = "jdbc:mysql://master:3306/todays-tableau"
  val user = "root"
  val password = "liushuai"


  def main(args: Array[String]): Unit = {
//    hiveAuth()

    Class.forName("com.mysql.jdbc.Driver")
    val conn: Connection = DriverManager.getConnection(jdbcUrl, user, password)
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
      .enableHiveSupport()
      .getOrCreate()

    val sc: spark.SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    val jedis = jedislocalConn()
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
      val country = jsonObj.getString("country")
      val time = jedis.zscore("device:public_track", jsonStr).toLong.toString.substring(0, 10).toLong
      val location = Location(uuid, msr_id, country, city, district, time)
      list = list :+ location

      if (list.size >= 1000) {
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
    jedis.disconnect()
    sc.stop()
    session.close()
  }


  def jedislocalConn(): Jedis = {
    println("开始连接redis local: master")
    val pool = new JedisPool("master", 6379)
    val jedis: Jedis = pool.getResource
    jedis.auth("liushuai")
    println("redis 连接成功")
    println(s"ping redis: ${jedis.ping()}")
    jedis
  }

  def hiveAuth(): Unit = {
    println("hive starting kerberos...... ")
    val conf = new Configuration
    conf.set("hadoop.security.authentication", "Kerberos")
    System.setProperty("java.security.krb5.conf", "krb5.conf")
    try {
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("user", "keytab")
    } catch {
      case e: Exception => println(s"hive kerberos auth failure, ${e.toString}")
    }
    println("hive kerberos auth success!")
  }

  def writeHive(session: SparkSession, sc: spark.SparkContext, dim_df: DataFrame, list: List[Location]): Unit = {
    val rdd = sc.parallelize(list)
    import session.implicits._
    val resultDf = rdd.toDF().join(dim_df, Seq("city"), "left")
      .selectExpr(exprs = "uuid", "msr_id", "country", "province", "city", "district", "time")
    resultDf.show(10)
    resultDf.createOrReplaceTempView("tmpTable")
    session.sql(
      """
        |insert into table dv_report.dw_ams_cust_location_tmp
        |select * from tmpTable
        |""".stripMargin)
  }
}
