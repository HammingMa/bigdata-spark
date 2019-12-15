package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperGlom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,3)

    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array => println(array.mkString(" ")))


    sc.stop()
  }
}
