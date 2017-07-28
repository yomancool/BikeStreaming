package receiver

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import java.io.{BufferedReader, InputStreamReader}
import org.apache.spark._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client._
import scala.collection.mutable.ArrayBuffer
import java.io.OutputStream
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream

class citiBikeReceiver(url:String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
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
        while(!isStopped()) {
        val conn = (new URL(url)).openConnection()
        conn.setConnectTimeout(timeout)
        conn.setReadTimeout(timeout)
        val stream = conn.getInputStream()
        var src = (scala.util.control.Exception.catching(classOf[Throwable]) opt Source.fromInputStream(stream).mkString) match {
          case Some(s: String) => s
          case _ => ""
        }
        store(src)
        Thread.sleep(3000)
        }
        //stream.close()
    } catch {
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
