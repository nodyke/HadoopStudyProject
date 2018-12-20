package com.dbocharov.test.spark.jobs

import com.dbocharov.spark.jobs.DataFrameCountJob
import com.dbocharov.spark.utils.{GeoUtils, SparkUtils}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row, types}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DataframeJobTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterAll{
  val event_path = "events.csv"
  val geodata_path = "geodata.csv"
  var df:DataFrame = null

  val excepted_category_report = List(
    Row("Game platform",680L),
    Row("Notebook",677L),
    Row("Smartphone",483L))

  val df_category_report_schema = StructType(Array(
    StructField("category",StringType,true),
    StructField("cnt",LongType,false)
  ))


  val excepted_product_report =  List(
    Row("Other","Headphones",246L),
    Row("Tablet","Ipad",224L),
    Row("Game platform","PS4",232L),
    Row("Game platform","PC",231L),
    Row("Smartphone","Samsung Galaxy",256L),
    Row("Smartphone","Iphone",227L),
    Row("Notebook","Macbook air",237L),
    Row("Notebook","Xiaomi Notebook",226L))

  val df_product_report_schema = StructType(Array(
    StructField("category",StringType,true),
    StructField("product_name",StringType,true),
    StructField("cnt",LongType,false)
  ))

  val excepted_country_report = List(
    Row("США",37913000L),
    Row("Австралия",14386000L),
    Row("Китай",8231000L),
    Row("Япония",5742000L),
    Row("Германия",2945000L),
    Row("Великобритания",2853000L),
    Row("Южная Корея",2606000L),
    Row("Канада",2385000L),
    Row("Франция",2232000L),
    Row("Бразилия",1830000L))

  val df_country_report_schema = StructType(Array(
    StructField("country_name",StringType,true),
    StructField("s",LongType,true)
  ))

  test("Test count most freq category"){
    val excepted = sqlContext.createDataFrame(sc.parallelize(excepted_category_report),df_category_report_schema)
    val result = DataFrameCountJob.countMostFreqCategories(sqlContext,df)
    assertDataFrameEquals(excepted,result)
  }
  test("Test count most freq product in each category"){
    val result = DataFrameCountJob.countMostFreqProductsInEachCategory(sqlContext,df).collect().toList
    assert(excepted_product_report.sortBy(row=>row.getString(1))==result.sortBy(row=>row.getString(1)))
  }

  test("Test count most purchased country"){
    val excepted = sqlContext.createDataFrame(sc.parallelize(excepted_country_report),df_country_report_schema)
    val geodata_map = sc.broadcast(GeoUtils.initGeodataMap(sc, geodata_path))
    val result = DataFrameCountJob.countMostFreqCountries(sqlContext,df,geodata_map)
    assertDataFrameEquals(excepted,result)
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    import sqlContext.implicits._
    df = SparkUtils.readEvents(sc,event_path).toDF().cache()
  }
}
