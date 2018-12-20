package com.dbocharov.spark.jobs

import java.util

import com.dbocharov.spark.config.{CountConfig, DatabaseConfig, PathConfig}
import com.dbocharov.spark.model.Event
import com.dbocharov.spark.utils.GeoUtils.GeoRange
import com.dbocharov.spark.utils.{GeoUtils, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

import scala.collection.mutable.ListBuffer


object RddCountJob {


  case class TopNCategoryReport(category: String,
                                count: Long)

  case class TopNProductInEachCatReport(category: String,
                                        product_name: String,
                                        count: Long)

  case class TopNCountryReport(country: String,
                               count: Long)


  def main(args: Array[String]): Unit = {
    Class.forName(DatabaseConfig.DRIVER)
    import com.dbocharov.spark.utils.DatabaseUtils._
    val sc = SparkUtils.initSparkContext(this.getClass.getName)
    val geodata_map = sc.broadcast(GeoUtils.initGeodataMap(sc,PathConfig.HDFS_GEODATA_PATH))
    val rdd = SparkUtils.readEvents(sc,PathConfig.HDFS_EVENT_PATH).cache()
    countMostFreqCategories(rdd)
      .writeToDb(DatabaseConfig.DB_URL, DatabaseConfig.USER, DatabaseConfig.PASSWORD)
    countMostFreqProductInEachCategory(rdd)
      .writeToDb(DatabaseConfig.DB_URL, DatabaseConfig.USER, DatabaseConfig.PASSWORD)
    countMostCountry(rdd, geodata_map.value)
      .writeToDb(DatabaseConfig.DB_URL, DatabaseConfig.USER, DatabaseConfig.PASSWORD)
  }

  def countMostFreqCategories(rdd: RDD[Event]) = {
    rdd
      .map(event => (event.category, 1))
      .reduceByKey((accum, n) => accum + n)
      .map(tuple => TopNCategoryReport(tuple._1, tuple._2))
      .takeOrdered(CountConfig.NUM_CATEGORIES)(Ordering[Long].reverse.on(report => report.count))
  }

  def countMostFreqProductInEachCategory(rdd: RDD[Event]) = {
    val result = new ListBuffer[TopNProductInEachCatReport]()
    rdd
      .map(event => ((event.category, event.product_name), 1))
      .reduceByKey((accum, sum) => accum + sum)
      .map(tuple => ((tuple._1._1), (tuple._1._2, tuple._2))) //map to tuple like (event.category,(event.product_name,count))
      .topByKey(CountConfig.NUM_PRODUCTS)(Ordering[Int].on(tuple=>tuple._2))
      .collect()
      .foreach(tuple => tuple._2.foreach(pair => result += TopNProductInEachCatReport(tuple._1, pair._1, pair._2))) // Map to case class
    result.toArray
  }

  def countMostCountry(rdd: RDD[Event], geodata_map: util.TreeMap[GeoRange, String]) = {
    rdd
      .map(event => (GeoUtils.countrySearch(event.ip, geodata_map), event.price))
      .filter(_._1.isDefined)
      .reduceByKey((accum, sum) => accum + sum)
      .map(tuple => TopNCountryReport(tuple._1.get, tuple._2))
      .takeOrdered(CountConfig.NUM_COUNTRIES)(Ordering[Long].reverse.on(report => report.count))
  }


}
