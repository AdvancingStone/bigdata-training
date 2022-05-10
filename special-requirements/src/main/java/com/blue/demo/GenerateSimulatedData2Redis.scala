package com.blue.demo

import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.JedisPool

import java.util.UUID
import scala.util.Random

object GenerateSimulatedData2Redis {
  def main(args: Array[String]): Unit = {
    val pool = new JedisPool("master", 6379)
    val jedis = pool.getResource
    jedis.auth("")

    val random = new Random()

    val cityList = List("北京市", "上海市", "昆明市", "合肥市", "运城市")

    for (i <- 1 to 10) {
      for (j <- 1000 to 1010) {
        val key = "device:public_track"
        val timestamp = System.currentTimeMillis()
        val id = UUID.randomUUID().toString
        val customerId = s"customerId${i}${j}"
        val action = "launch"
        val location =
          s"""["${random.nextFloat()}","${random.nextFloat()}"]""".stripMargin
        val cityCode = s"${i + j}"
        val adCode = s"${i}${j}"
        val cityName = cityList(random.nextInt(5))
        val district = s"district${i}"
        val country = "中国"
        val json = locationJson(id, customerId, action, location, cityCode, adCode, cityName, district, country)
        println(json)
        jedis.zadd(key, timestamp, json)
        //        Thread.sleep(100)
      }
    }


  }

  private def locationJson(id: String, customerId: String, action: String, location: String, cityCode: String, adCode: String, cityName: String, district: String, country: String): String = {
    val json = new JSONObject
    json.put("id", id)
    json.put("customerId", customerId)
    json.put("type", "Track")
    json.put("deviceId", "123456")
    json.put("deviceType", "ios")
    json.put("lang", "zh")
    json.put("action", action)
    json.put("location", location)
    json.put("cityCode", cityCode)
    json.put("adCode", adCode)
    json.put("cityName", cityName)
    json.put("district", district)
    json.put("country", country)
    json.toString()
  }
}
