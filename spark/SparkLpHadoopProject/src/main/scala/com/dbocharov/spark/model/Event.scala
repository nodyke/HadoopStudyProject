package com.dbocharov.spark.model

import java.sql.Timestamp
case class Event(
                  product_name:String,
                  price:Long,
                  date:Timestamp,
                  category:String,
                  ip:String
                )
