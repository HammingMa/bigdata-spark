package com.mzh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming word count")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hdp1",9999)

    val flatMapDStream: DStream[String] = socketDStream.flatMap(_.split(" "))
    val mapDStream: DStream[(String, Int)] = flatMapDStream.map((_,1))
    val resultDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
