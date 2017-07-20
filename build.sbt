name := "BikeJob"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "org.apache.spark" %% "spark-streaming" % "1.6.0",
    "org.json4s" %% "json4s-jackson" % "3.2.10",
    "joda-time" % "joda-time" % "2.3",
    "org.joda" % "joda-convert" % "1.7",
    "com.ning" % "async-http-client" % "1.9.10",
    "org.json4s" %% "json4s-native" % "3.2.11",
    "org.yaml" % "snakeyaml" % "1.15",
    "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)
