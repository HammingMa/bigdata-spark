package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MakeRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    //val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),2)
    listRDD.collect().foreach(println)

    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4,6))
    println(arrayRDD.collect().mkString(" "))

    val linesRDD: RDD[String] = sc.textFile("input",1)
    linesRDD.collect().foreach(println)

    //listRDD.saveAsTextFile("output")
    linesRDD.saveAsTextFile("output")

    sc.stop()
  }
}
