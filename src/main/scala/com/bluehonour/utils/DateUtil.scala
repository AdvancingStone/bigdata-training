package com.bluehonour.utils

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

object DateUtil {


  /**
   * 将时间格式化为 yyyy-MM-dd 格式
   */
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  /**
   * 将时间格式化为 yyyyMMdd 格式
   */
  val dateZipFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  /**
   * 将时间格式化为 yyyy-MM-dd hh:mm:ss 格式
   */
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")

  /**
   * 当日时间
   */
  val today: String = dateFormatter.format(LocalDateTime.now)

  /**
   * 当日时间
   */
  val todayZip: String = dateZipFormatter.format(LocalDateTime.now)


  def dateToMilli(date: String, format: DateTimeFormatter = dateFormatter): Long = {
    toMilli(dateTo(date, format))
  }

  def toMilli(date: String, format: DateTimeFormatter = dateTimeFormatter): Long = {
    toMilli(dateTimeTo(date, format))
  }

  def toMilli(localDateTime: LocalDateTime): Long = {
    localDateTime.truncatedTo(ChronoUnit.DAYS)
    localDateTime.toInstant(ZoneOffset.UTC)
      .toEpochMilli
  }

  def parseDate(date: String, format: DateTimeFormatter = dateFormatter): LocalDate = {
    LocalDate.parse(date, format)
  }

  def parseDateTime(dateTime: String, format: DateTimeFormatter = dateTimeFormatter): LocalDateTime = {
    LocalDateTime.parse(dateTime, format)
  }

  def to(localDate: LocalDate): LocalDateTime = {
    localDate.atStartOfDay()
  }

  def dateTo(date: String, format: DateTimeFormatter = dateFormatter): LocalDateTime = {
    to(parseDate(date, format))
  }

  def dateTimeTo(date: String, format: DateTimeFormatter = dateTimeFormatter): LocalDateTime = {
    parseDateTime(date, format)
  }
}
