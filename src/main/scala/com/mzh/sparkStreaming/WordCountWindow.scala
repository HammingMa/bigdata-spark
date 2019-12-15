package com.mzh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountFromKafka").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.sparkContext.setCheckpointDir("./cp")

    val kafakDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,"hdp1:2181","wordCount",Map(("words",3)))

    val windowDStream: DStream[(String, String)] = kafakDStream.window(Seconds(9),Seconds(3))

    val flatMapDstream: DStream[String] = windowDStream.flatMap(_._2.split(" "))

    val mapDStream: DStream[(String, Int)] = flatMapDstream.map((_,1))


    val resDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)



    resDStream.print()

    resDStream.saveAsTextFiles("./out")

    ssc.start()
    ssc.awaitTermination()
  }
}
