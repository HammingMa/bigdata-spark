package com.mzh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStatus {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountFromKafka").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.sparkContext.setCheckpointDir("./cp")

    val kafakDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,"hdp1:2181","wordCount",Map(("words",3)))

    val flatMapDstream: DStream[String] = kafakDStream.flatMap(_._2.split(" "))

    val mapDStream: DStream[(String, Int)] = flatMapDstream.map((_,1))

    val statusDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buff) => {
        val sum = buff.getOrElse(0) + seq.sum
        Option(sum)
      }
    }



    statusDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
