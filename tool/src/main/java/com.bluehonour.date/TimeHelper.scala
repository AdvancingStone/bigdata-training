package com.bluehonour.date

import org.joda.time.DateTime

object TimeHelper {
  def main(args: Array[String]): Unit = {
    val startDate = TimeHelper.addDay(TimeHelper.getTodayDate, -2)
    val endDate = TimeHelper.getTodayDate
    println(startDate, endDate)
  }

  def getDate(dateString: String): DateTime = {
    DateTime.parse(dateString)
  }

  def addDay(date: String, days: Int): String = {
    new DateTime(date).plusDays(days).toString("yyyy-MM-dd")
  }

  def addDirDay(date: String, days: Int): String = {
    new DateTime(date).plusDays(days).toString("yyyyMMdd")
  }

  def addWeek(date: String, weeks: Int): String = {
    new DateTime(date).plusWeeks(weeks).toString("yyyy-MM-dd")
  }

  def getTodayDate: String = {
    new DateTime().toString("yyyy-MM-dd")
  }

  def getTodayTime: String = {
    new DateTime().toString("yyyy-MM-dd HH:mm:ss")
  }

  def getTodayHour(date: Long): String = {
    new DateTime(date).toString("HH")
  }

  def getNowHour: String = {
    new DateTime().toString("HH")
  }

  def getYesterday: String = {
    new DateTime().plusDays(-1).toString("yyyy-MM-dd")
  }

  def getLastWeek: String = {
    new DateTime().plusWeeks(-1).toString("yyyy-MM-dd")
  }

  def getTimestampByDate(date: String): Long = {
    new DateTime(date).getMillis / 1000L
  }

  def getNowTimestamp: Long = {
    new DateTime().getMillis
  }

  def get1stDayOfWeek(date: String): String = {
    val days = new DateTime(date).getDayOfWeek
    addDay(date, 1 - days)
  }

  def getLastDayOfWeek(date: String): String = {
    val days = new DateTime(date).getDayOfWeek
    addDay(date, 7 - days)
  }

}
