package com.dbocharov.test.spark.jobs

import com.dbocharov.spark.jobs.RddCountJob
import com.dbocharov.spark.jobs.RddCountJob.{TopNCategoryReport, TopNCountryReport, TopNProductInEachCatReport}
import com.dbocharov.spark.model.Event
import com.dbocharov.spark.utils.{GeoUtils, SparkUtils}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class RddJobTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {
  val event_path = "events.csv"
  val geodata_path = "geodata.csv"
  var rdd:RDD[Event] = null

  val excepted_category_report = List(TopNCategoryReport("Game platform",680),
  TopNCategoryReport("Notebook",677),
  TopNCategoryReport("Smartphone",483))

  val excepted_product_report =  List(TopNProductInEachCatReport("Other","Headphones",246),
  TopNProductInEachCatReport("Tablet","Ipad",224),
  TopNProductInEachCatReport("Game platform","PS4",232),
  TopNProductInEachCatReport("Game platform","PC",231),
  TopNProductInEachCatReport("Smartphone","Samsung Galaxy",256),
  TopNProductInEachCatReport("Smartphone","Iphone",227),
  TopNProductInEachCatReport("Notebook","Macbook air",237),
  TopNProductInEachCatReport("Notebook","Xiaomi Notebook",226))

  val excepted_country_report = List(
  TopNCountryReport("США",37913000),
  TopNCountryReport("Австралия",14386000),
  TopNCountryReport("Китай",8231000),
  TopNCountryReport("Япония",5742000),
  TopNCountryReport("Германия",2945000),
  TopNCountryReport("Великобритания",2853000),
  TopNCountryReport("Южная Корея",2606000),
  TopNCountryReport("Канада",2385000),
  TopNCountryReport("Франция",2232000),
  TopNCountryReport("Бразилия",1830000))

  test("Test count most categories") {
    val category_reports =RddCountJob.countMostFreqCategories(rdd)
    assert(category_reports.toList.sortBy(_.category) == excepted_category_report.sortBy(_.category))
  }

  test("Test count most product in each category"){
    val product_reports = RddCountJob.countMostFreqProductInEachCategory(rdd)
    assert(product_reports.toList.sortBy(_.category)==excepted_product_report.sortBy(_.category))
  }

  test("Test count most country"){
    val geodata = GeoUtils.initGeodataMap(sc,geodata_path)
    val country_reports = RddCountJob.countMostCountry(rdd,geodata)
    assert(country_reports.toList.sortBy(_.country)==excepted_country_report.sortBy(_.country))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    rdd = SparkUtils.readEvents(sc,event_path).cache()
  }
}
