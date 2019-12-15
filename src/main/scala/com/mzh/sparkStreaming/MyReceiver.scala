package com.mzh.sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

object MyReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming word count")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val my: ReceiverInputDStream[String] = ssc.receiverStream(new MySocktReceiver("hdp1",9999))

    val flatMapDStream: DStream[String] = my.flatMap(_.split(" "))
    val mapDStream: DStream[(String, Int)] = flatMapDStream.map((_,1))
    val resultDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}

class MySocktReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2) {

  var socket: Socket = null

  def recieiver(): Unit ={
    socket = new Socket(host,port)
    val reader: BufferedReader = new BufferedReader((new InputStreamReader(socket.getInputStream())))
    var line : String = null
    while ((line = reader.readLine())!=null){
      if("END".equals(line)){
        return
      }else{
        this.store(line)
      }
    }

  }

  override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          recieiver()
        }
      }).start()
  }

  override def onStop(): Unit = {
    if(socket!=null){
     socket.close()
      socket=null
    }
  }
}

