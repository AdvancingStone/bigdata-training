package com.bluehonour.demo.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DateUtil {
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val today: String = dateFormatter.format(LocalDateTime.now)
}
