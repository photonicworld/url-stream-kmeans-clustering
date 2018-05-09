package com.classifier.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object DateUtils {

  def toDateString(date: DateTime, format: String = "YYYY-MM-dd'T'HH:mm:ss"): String = {
    date.toString(DateTimeFormat.forPattern(format))
  }


}
