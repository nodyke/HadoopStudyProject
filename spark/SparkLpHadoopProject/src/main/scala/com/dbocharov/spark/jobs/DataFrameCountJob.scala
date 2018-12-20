package com.dbocharov.spark.jobs

import java.util

import com.dbocharov.spark.config.{CountConfig, DatabaseConfig, PathConfig}
import com.dbocharov.spark.utils.{GeoUtils, SparkUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}


object DataFrameCountJob {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.initSparkContext(this.getClass.getName)
    sc.setCheckpointDir(PathConfig.CHECKPOINT_PATH)

    val geodata_map = sc.broadcast(GeoUtils.initGeodataMap(sc, PathConfig.HDFS_GEODATA_PATH))
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    val df = hiveContext.createDataFrame(SparkUtils.readEvents(sc, PathConfig.HDFS_EVENT_PATH)).cache()

    countMostFreqCategories(sqlContext, df).writeToJdbc(DatabaseConfig.TOP_CATEGORIES_TABLE)
    countMostFreqProductsInEachCategory(hiveContext, df).writeToJdbc(DatabaseConfig.TOP_PRODUCT_TABLE)
    countMostFreqCountries(sqlContext, df, geodata_map).writeToJdbc(DatabaseConfig.TOP_COUNTRY_TABLE)
  }


  def countMostFreqCategories(sqlContext: SQLContext, df: DataFrame) = {
    import sqlContext.implicits._
    df.select($"category")
      .groupBy($"category")
      .count()
      .select($"category", $"count" as "cnt")
      .orderBy($"cnt".desc)
      .limit(CountConfig.NUM_CATEGORIES)
  }

  def countMostFreqProductsInEachCategory(hiveContext: HiveContext, df: DataFrame) = {
    import hiveContext.implicits._
    val windowSpec = Window.partitionBy($"category").orderBy($"count".desc)
    df.select($"category", $"product_name")
      .groupBy($"category", $"product_name")
      .count()
      .withColumn("num", row_number().over(windowSpec))
      .where($"num" <= CountConfig.NUM_PRODUCTS)
      .drop($"num")
      .withColumnRenamed("count","cnt")
  }


   def countMostFreqCountries(sqlContext: SQLContext,
                                     df: DataFrame,
                                     geodata_map: Broadcast[util.TreeMap[GeoUtils.GeoRange, String]]) = {
    val getCountryName = (ip: String) => {
      val option = GeoUtils.countrySearch(ip, geodata_map.value)
      option.getOrElse(GeoUtils.NOT_FOUND_COUNTRY)
    }
    import org.apache.spark.sql.functions.udf
    import sqlContext.implicits._
    val country_search = udf(getCountryName)
    df.select(country_search($"ip") as "country_name", $"price")
      .where($"country_name".notEqual(GeoUtils.NOT_FOUND_COUNTRY))
      .groupBy("country_name")
      .sum("price")
      .select($"country_name", $"sum(price)" as "s")
      .orderBy($"s".desc)
      .limit(CountConfig.NUM_COUNTRIES)
  }

  implicit class DataframeDbWriter(df: DataFrame) {
    def writeToJdbc(table: String) = {
      df.write
        .mode("append")
        .jdbc(DatabaseConfig.DB_URL, table, DatabaseConfig.CONNECTION_PROPERTIES)
    }
  }


}
