package util

import core._
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._
import org.joda.time.DateTime
import org.apache.spark.Partitioner
import org.apache.spark.streaming.Seconds
import scala.util.Try

object Parsing {


  @transient implicit val formats = DefaultFormats

  def parseInfo(infoJson: String)={
    Try({
      val json=parse(infoJson)
      val data=(json \ "data") \ "stations"
      val capacity = (data \ "capacity").extract[List[Double]]
      val lon = (data \ "lon").extract[List[Double]]
      val lat = (data \ "lat").extract[List[Double]]
      val station_id = (data \ "station_id").extract[List[String]]
      (capacity, lon, lat, station_id)
    }).toOption
  }

  def parseStatus(statusJson: String)={
    Try({
        val json=parse(statusJson)
        val data=(json \ "data") \ "stations"
        val num_bikes_available = (data \ "num_bikes_available").extract[List[Double]]
        val num_bikes_disabled = (data \ "num_bikes_disabled").extract[List[Double]]
        val num_docks_available = (data \ "num_docks_available").extract[List[Double]]
        val num_docks_disabled = (data \ "num_docks_disabled").extract[List[Double]]
        val station_id = (data \ "station_id").extract[List[String]]
        (num_bikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, station_id)
    }).toOption
  }
}
