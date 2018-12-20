package com.dbocharov.spark.utils


import java.sql.Timestamp

import com.dbocharov.spark.model.Event
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDateTime

object SparkUtils {
  def initSparkContext(app_name: String) = {
    val sparkConf = new SparkConf()
      .setAppName(app_name)
      .setMaster("local[*]")
    new SparkContext(sparkConf)
  }

  def readEvents(sc: SparkContext, event_path: String) = {
    sc.textFile(event_path)
      .map(csv => csv.parseEventCsvString())
      .filter(option => option.isDefined)
      .map(option => option.get)
  }


  implicit class EventCsvParser(csv: String) {
    def parseEventCsvString(): Option[Event] = {
      try {
        val temp = csv.split(",")
        Option.apply[Event](new Event(
          temp(0),
          temp(1).toLong,
          new Timestamp(LocalDateTime.parse(temp(2)).toDate.getTime),
          temp(3),
          temp(4)
        ))
      }
      catch {
        case e: Throwable => {
          println(e); Option.empty[Event]
        }
      }
    }

  }

}
