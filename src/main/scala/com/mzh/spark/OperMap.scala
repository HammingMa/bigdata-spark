package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val mapRDD: RDD[Int] = listRDD.map(_*2)

    println(mapRDD.collect().mkString(" "))

    sc.stop()
  }
}
