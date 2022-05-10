package com.blue.utils

import java.text.SimpleDateFormat
import java.util.Date

object Utils {
  def getDateMin(date: Date): String = {
    val pattern = "yyyyMMddHHmm"
    val dateFormat = new SimpleDateFormat(pattern)
    val min = dateFormat.format(date)
    min
  }

}
