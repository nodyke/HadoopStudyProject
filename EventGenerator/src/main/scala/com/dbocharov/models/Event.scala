package com.dbocharov.models

import java.time.LocalDateTime

case class Event(
                  name: String,
                  price: Long,
                  date: LocalDateTime,
                  category: String,
                  ip: String
                )