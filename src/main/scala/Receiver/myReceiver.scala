package receiver

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import java.io.{BufferedReader, InputStreamReader}
import org.apache.spark._

class citiBikeReceiver(url:String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("bike receiver") {
      override def run() { receive(url,3000) }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself isStopped() returns false
  }

  private def receive(url: String, timeout: Int) {
    import java.net.URL
    import scala.io.Source
    try {
        val conn = (new URL(url)).openConnection()
        conn.setConnectTimeout(timeout)
        conn.setReadTimeout(timeout)
        val stream = conn.getInputStream()
        var src = (scala.util.control.Exception.catching(classOf[Throwable]) opt Source.fromInputStream(stream).mkString) match {
          case Some(s: String) => s
          case _ => ""
        }
        while(src!=null) {
            store(src)
            Thread.sleep(3000)
            src = (scala.util.control.Exception.catching(classOf[Throwable]) opt Source.fromInputStream(stream).mkString) match {
              case Some(s: String) => s
              case _ => ""
            }
        }
        stream.close()
    } catch {
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
