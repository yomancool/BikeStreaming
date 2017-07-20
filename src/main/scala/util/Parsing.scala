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
      val json=parse(infoJson).camelizeKeys
      val member=(json \ "member").extract[Member]
      val event=(json \ "event").extract[MemberEvent]
      val response=(json \ "response").extract[String]
      (member, event, response)
    }).toOption
  }

  def parseStatus(statusJson: String)={
    Try({
      val json=parse(statusJson).camelizeKeys
      val member=(json \ "member").extract[Member]
      val event=(json \ "event").extract[MemberEvent]
      val response=(json \ "response").extract[String]
      (member, event, response)
    }).toOption
  }

}
