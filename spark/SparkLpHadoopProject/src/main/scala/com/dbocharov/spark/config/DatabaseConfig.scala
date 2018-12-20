package com.dbocharov.spark.config

import java.util.Properties

object DatabaseConfig {
  val TOP_CATEGORIES_TABLE="top_categories"
  val TOP_PRODUCT_TABLE="top_product_in_each_cat"
  val TOP_COUNTRY_TABLE="top_country"
  val DB_URL = "jdbc:postgresql://10.0.2.2:5432/report"
  val USER = "dbocharov"
  val PASSWORD = "test"
  val DRIVER = "org.postgresql.Driver"
  val CONNECTION_PROPERTIES = {
    val prop = new Properties()
    prop.setProperty("user",USER)
    prop.setProperty("password",PASSWORD)
    prop.setProperty("driver",DRIVER)
    prop
  }
}
