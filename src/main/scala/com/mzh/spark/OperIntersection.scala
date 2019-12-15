package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperIntersection {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD1: RDD[Int] = sc.makeRDD(3 to 8)
    val listRDD2: RDD[Int] = sc.makeRDD(6 to 10)

    val intersectionRDD: RDD[Int] = listRDD1.intersection(listRDD2)


    println(intersectionRDD.collect().mkString(" "))

    sc.stop()
  }
}
