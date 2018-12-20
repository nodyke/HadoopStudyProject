package com.dbocharov.utils

object Dictionary {

  val ERROR_WRITE_SOCKET_MESSAGE="Error write to socket"
  val ERROR_CLOSE_OUTPUT_STREAM_MESSAGE="Error closing output stream"
  val ERROR_CLOSE_SOCKET_MESSAGE="Error closing tcp socket"
  val ERROR_PARSING_MESSAGE="Error parsing"
  //Map products for generate, key - product name, value - pair(price, category_id)
  val products:Map[String,(Long,Int)] = Map(
    "PC" -> (50000,3),
    "PS4" -> (30000,3),
    "XBOXOne" ->(20000,3),
    "Iphone" -> (40000,2),
    "Headphones" -> (5000,5),
    "Samsung Galaxy" -> (30000,2),
    "Ipad" -> (28000,4),
    "Macbook air" -> (70000,1),
    "Macbook pro" ->(120000,1),
    "Xiaomi Notebook" -> (70000,1)
    )

  val categories:Map[Int,String] = Map(
    1 -> "Notebook",
    2 -> "Smartphone",
    3 -> "Game platform",
    4 -> "Tablet",
    5 -> "Other"
  )
}
