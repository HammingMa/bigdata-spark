package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Practice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    val sc = new SparkContext(conf)

    val fileRDD: RDD[String] = sc.textFile("./input/agent.log")

    val mapRDD: RDD[(String, Int)] = fileRDD.map { line => {
      val words: Array[String] = line.split(" ")
      (words(1) + "-" + words(4), 1)
    }
    }

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    val mapRDD2: RDD[(String, (String, Int))] = reduceByKeyRDD.map(t =>(t._1.split("-")(0),(t._1.split("-")(1),t._2)))

    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    val resRDD: RDD[(String, Iterable[(String, Int)])] = groupByKeyRDD.mapValues(_.toList.sortWith(_._2 >_._2).take(3))

    println(resRDD.collect().mkString(" "))

  }
}
