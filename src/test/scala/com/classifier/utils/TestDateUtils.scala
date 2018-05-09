package com.classifier.utils

import com.classifier.ElasticsearchRddHandler
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FlatSpec}


class TestDateUtils extends FlatSpec with BeforeAndAfter {
  var date = new DateTime()

  before {
    date = new DateTime()
      .withYear(2016)
      .withMonthOfYear(10)
      .withDayOfMonth(15)
      .withHourOfDay(17)
      .withMinuteOfHour(0)
      .withSecondOfMinute(0);
  }

  it should "format date as string" in {
    val dateTimeStr = DateUtils.toDateString(date)

    assert(dateTimeStr == "2016-10-15T17:00:00")
  }

  it should "parse date for a given format" in {
    val dateTimeString = DateUtils.toDateString(date, ElasticsearchRddHandler.ES_DATE_INDEX_FORMAT)
    assert(dateTimeString == "2016-10-15")
  }

}
