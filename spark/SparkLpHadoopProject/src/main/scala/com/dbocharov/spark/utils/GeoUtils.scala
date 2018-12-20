package com.dbocharov.spark.utils

import java.util
import java.util.Comparator

import org.apache.spark.SparkContext

object GeoUtils extends Serializable {


  val NOT_FOUND_COUNTRY = "none"

  case class GeoRange(start: Long,
                      end: Long)

  private class GeodataComparator extends Comparator[GeoRange] with Serializable {
    override def compare(o1: GeoRange, o2: GeoRange): Int = o2.start.compareTo(o1.end)
  }

  def countrySearch(ip: String, geodata_map: util.TreeMap[GeoRange, String]) = {
    val int_ip = ipToInt(ip)
    val entry = geodata_map.higherEntry(new GeoRange(int_ip, int_ip))
    if (entry != null) Option[String](entry.getValue)
    else Option.empty[String]
  }

  def ipToInt(ip: String) = {
    val temp = ip.split("\\.")
    var result = 0l
    for (i <- 0 to (temp.length - 1)) {
      val power = 3 - i
      val _ip = temp(i).toInt
      result = result + (_ip * Math.pow(256, power)).toLong
    }
    result
  }

  def initGeodataMap(sc: SparkContext, geodata_path: String) = {
    val tree_geo_map = new util.TreeMap[GeoRange, String](new GeodataComparator())
    sc.textFile(geodata_path)
      .map(s => s.split(","))
      .collect()
      .foreach(csv =>
        {
          try {
            tree_geo_map.put(new GeoRange(csv(0).toLong, csv(1).toLong), csv(3))
          }
          catch {case e:Throwable=>println(e)}
        })
    tree_geo_map
  }


}
