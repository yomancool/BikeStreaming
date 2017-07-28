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

object BikeJob {
  def main(args: Array[String]) {
    import util.Parsing._

    StreamingExamples.setStreamingLogLevels()

    val sparkConf = new SparkConf()
        .setAppName("BikeJob")
        .setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val station_status = ssc.receiverStream(new citiBikeReceiver("https://gbfs.citibikenyc.com/gbfs/en/station_status.json")).flatMap(parseStatus)
    val station_info = ssc.receiverStream(new citiBikeReceiver("https://gbfs.citibikenyc.com/gbfs/en/station_information.json")).flatMap(parseInfo)

    println("start streaming")

    //processing data
    val status_pair = station_status.map{case(num_bikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, station_id) => (station_id, (num_bikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled))}
    val info_pair = station_info.map{case(capacity, lon, lat, station_id) => (station_id, (capacity, lon, lat))}
    val station_pair = status_pair.join(info_pair).map{case(station_id, (status, info)) => (station_id, status._1,status._2,status._3,status._4,info._1,info._2,info._3)}

    val stations = station_pair.map{case(station_id ,a,b,c,d,e,f,g) => station_id.zip(a.zip(b.zip(c.zip(d.zip(e.zip(f.zip(g))))))).map{case(id, (aa, (bb, (cc, (dd, (ee, (ff, gg))))))) => (id, (aa,bb,cc,dd,ee,ff,gg))}}

    stations.print()
    //stations.foreachRDD(x => println("stations count ===>" + x.count))
    // station schema
    // (id, (num_bikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, capacity, lon, lat))

    ssc.start()
    ssc.awaitTermination()
  }
}
