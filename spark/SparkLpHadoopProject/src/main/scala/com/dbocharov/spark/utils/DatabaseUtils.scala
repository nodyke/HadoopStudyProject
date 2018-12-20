package com.dbocharov.spark.utils


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.dbocharov.spark.config.DatabaseConfig
import com.dbocharov.spark.jobs.RddCountJob.{TopNCategoryReport, TopNCountryReport, TopNProductInEachCatReport}

object DatabaseUtils {


  private val sql_insert_category_report_statements = s"INSERT INTO public.${DatabaseConfig.TOP_CATEGORIES_TABLE} " +
    s"(category, cnt) VALUES (?, ?)"
  private val sql_insert_product_each_category_report_statements = s"INSERT INTO public.${DatabaseConfig.TOP_PRODUCT_TABLE} (" +
    s"category, product_name, cnt) VALUES (?, ?, ?)"
  private val sql_insert_country_report_statements = s"INSERT INTO public.${DatabaseConfig.TOP_COUNTRY_TABLE} " +
    s"(country_name, s) VALUES (?, ?)"


  implicit class CategoryReportWriter(array: Array[TopNCategoryReport]) {
    def writeToDb(db_url: String, user: String, password: String) = {
      var con: Connection = null
      try {
        con = DriverManager.getConnection(db_url, user, password)
        array.foreach(report => {
          var ps: PreparedStatement = null
          try {
            ps = con.prepareStatement(sql_insert_category_report_statements)
            ps.setString(1, report.category)
            ps.setLong(2, report.count)
            ps.executeUpdate()
          }
          finally {
            if (ps != null) ps.close()
          }
        })
      }
      finally {
        if (con != null) con.close()
      }
    }
  }

  implicit class ProductReportWriter(array: Array[TopNProductInEachCatReport]) {
    def writeToDb(db_url: String, user: String, password: String) = {
      var con: Connection = null
      try {
        con = DriverManager.getConnection(db_url, user, password)
        array.foreach(report => {
          var ps: PreparedStatement = null
          try {
            ps = con.prepareStatement(sql_insert_product_each_category_report_statements)
            ps.setString(1, report.category)
            ps.setString(2, report.product_name)
            ps.setLong(3, report.count)
            ps.executeUpdate()
          }
          finally {
            if (ps != null) ps.close()
          }
        })
      }
      finally {
        if (con != null) con.close()
      }
    }
  }

  implicit class CountryReportWriter(array: Array[TopNCountryReport]) {
    def writeToDb(db_url: String, user: String, password: String) = {
      var con: Connection = null
      try {
        con = DriverManager.getConnection(db_url, user, password)
        array.foreach(report => {
          var ps: PreparedStatement = null
          try {
            ps = con.prepareStatement(sql_insert_country_report_statements)
            ps.setString(1, report.country)
            ps.setLong(2, report.count)
            ps.executeUpdate()
          }
          finally {
            if (ps != null) ps.close()
          }
        })
      }
      finally {
        if (con != null) con.close()
      }
    }
  }

}
