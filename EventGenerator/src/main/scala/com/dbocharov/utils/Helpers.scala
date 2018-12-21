package com.dbocharov.utils

import java.time.LocalDateTime

import com.dbocharov.models.Event

import scala.util.Random

object Helpers {

  implicit class EventRandomizer(randomizer: Random) {
    def randomEvent(): Event = {
      def generateRandomDate(): LocalDateTime = {
        val now = LocalDateTime.now()
        now
          .minusWeeks(1)
          .minusHours(now.getHour)
          .minusMinutes(now.getMinute)
          .minusSeconds(now.getSecond)
          .plusDays(randomizer.nextInt(7))
          .plusHours(randomizer.nextInt(24))
          .plusMinutes(randomizer.nextInt(60))
          .plusSeconds(randomizer.nextInt(60))
      }

      def generateIp(): String = s"${randomizer.nextInt(255)}.${randomizer.nextInt(255)}.${randomizer.nextInt(255)}.${randomizer.nextInt(255)}"

      val product_name = Dictionary.products.keys.toList(randomizer.nextInt(Dictionary.products.size))
      val product_info = Dictionary.products(product_name) //product info is a pair of price and category

      Event(
        product_name,
        product_info._1, //get price
        generateRandomDate(),
        Dictionary.categories(product_info._2), //get category
        generateIp()
      )
    }
  }

  implicit class EventTransformer(event: Event) {
    def generateCsv(): String = s"${event.name},${event.price},${event.date},${event.category},${event.ip}"
  }

}
