package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // val conf = new SparkConf().setMaster("local").setAppName("WrodCount")
    val conf = new SparkConf().setAppName("WrodCount")

    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("input" )
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map((_,1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    val result: Array[(String, Int)] = wordToSum.collect()

    result.foreach(println)

    sc.stop()
  }
}
