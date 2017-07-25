package core

case class Venue(name: Option[String], address1: Option[String], city: Option[String], state: Option[String], zip: Option[String], country: Option[String], lon: Option[Float], lat: Option[Float]){
  def toExpandedCityString=List(city, state, country).flatMap{x=>x.map{_.toLowerCase()}}.mkString(",")
}

case class Event(id: String, name: Option[String], eventUrl: Option[String], description: Option[String], time: Option[Long], rsvpLimit: Option[Int])
case class Group(id: Option[String], category: Option[String], name: Option[String], city: Option[String], state: Option[String], country: Option[String])

case class Member(memberName: Option[String], memberId: Option[String])
case class MemberEvent(eventId: Option[String], eventName: Option[String], eventUrl: Option[String], time: Option[Long])

case class Status(num_bikes_available: Option[Long], num_bikes_disabled: Option[Long], num_docks_available: Option[Long], num_docks_disabled: Option[Long], station_id: Option[String])
case class Info(capacity: Option[Long], lat: Option[Long], lon: Option[Long], station_id: Option[String])
