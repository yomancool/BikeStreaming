package bikeJob

import receiver.citiBikeReceiver
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.io.Source
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Custom Receiver that receives data over a socket. Received bytes are interpreted as
 * text and \n delimited lines are considered as records. They are then counted and printed.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.CustomReceiver localhost 9999`
 */
object BikeJob {
  def main(args: Array[String]) {
    import util.Parsing._
    // if (args.length < 2) {
    //   System.err.println("Usage: CustomReceiver <hostname> <port>")
    //   System.exit(1)
    // }

    StreamingExamples.setStreamingLogLevels()

    val sparkConf = new SparkConf()
        .setAppName("BikeJob")
        .setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // val station_status = ssc.receiverStream(new citiBikeReceiver("https://gbfs.citibikenyc.com/gbfs/en/station_status.json")).flatMap(parseStatus)
    // val station_info = ssc.receiverStream(new citiBikeReceiver("https://gbfs.citibikenyc.com/gbfs/en/station_information.json")).flatMap(parseInfo)
    val station_status = ssc.receiverStream(new citiBikeReceiver("https://gbfs.citibikenyc.com/gbfs/en/station_status.json"))
    val station_info = ssc.receiverStream(new citiBikeReceiver("https://gbfs.citibikenyc.com/gbfs/en/station_information.json"))

    // // val words = lines.flatMap(_.split(" "))
    // val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    println("start streaming");
    station_status.foreachRDD(x => {println("station_status=====>"+ x.count());x.count()});
    station_info.foreachRDD(x => {println("station_info=====>"+ x.count());x.count()});

    //station_status.select(explode($"data.stations"))

    ssc.start()
    ssc.awaitTermination()
  }
}
