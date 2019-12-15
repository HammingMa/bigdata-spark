package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperFlatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

     val listRDD: RDD[Array[Int]] = sc.makeRDD(List(Array(1,2),Array(4,5)))

    val flatMapRDD: RDD[Int] = listRDD.flatMap(a => a)

    println(flatMapRDD.collect().mkString(" "))

    sc.stop()
  }
}
