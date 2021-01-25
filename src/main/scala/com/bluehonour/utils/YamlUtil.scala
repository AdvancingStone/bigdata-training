package com.bluehonour.utils

import java.util

import com.bluehonour.constant.JsonConstant
import scala.collection.JavaConverters._
import scala.collection.mutable

object YamlUtil {

  case class EmbeddedField(field: String, tier: Int)

  var strategy: String = JsonConstant.MULTI_FIELDS
  var flag: Boolean = true

  def getJsonFieldValue(map: util.HashMap[String, Object], field: String): util.HashMap[String, Object] = {
    val result: Object = map.get(field)
    if (result.isInstanceOf[util.HashMap[String, Object]]) result.asInstanceOf[util.HashMap[String, Object]] else null
  }

  def init(): util.HashMap[String, Object] = {
    val load: util.Map[String, Object] = LoadConfigurationFile.loadJsonYaml()
    val map = load.get(JsonConstant.JSON).asInstanceOf[util.HashMap[String, Object]]
    if (flag && map.get(JsonConstant.STRATEGY) != null) {
      strategy = map.get(JsonConstant.STRATEGY).toString
      flag = false
    }
    map
  }

  def getFieldsValue(field: String): Object = {
    val map = init()
    map.get(field)
  }

  def setJsonParseStrategy(field: String): Unit = {
    if (field != null && !field.trim.equals("")) {
      strategy = field
    }
  }

  def getJsonParseStrategy(): String = {
    strategy
  }

  def getMultiFieldsValue(field: String): List[String] = {
    val value = getFieldsValue(field)
    if (value != null) {
      val splits: Array[String] = value.toString.trim.split(",").map(x => x.trim)
      splits.toList
    } else null
  }

  def getEmbeddedFieldsValue(field: String): List[EmbeddedField] = {
    var result: List[EmbeddedField] = List()
    val list = getFieldsValue(field).asInstanceOf[util.ArrayList[mutable.HashMap[String, String]]]
    if (list != null) {
      val iterator = list.iterator
      while (iterator.hasNext) {
        val map = iterator.next().asInstanceOf[util.HashMap[String, String]].asScala
        val ef = EmbeddedField(map.get(JsonConstant.FIELD).get, map.get(JsonConstant.TIER).get.asInstanceOf[Int])
        result = result :+ ef
      }
    }
    result
  }
}
