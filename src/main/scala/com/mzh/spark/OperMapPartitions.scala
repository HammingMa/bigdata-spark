package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperMapPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(_.map(_*2))
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(p => p.map(x => x*2))

    println(mapPartitionsRDD.collect().mkString(" "))

    sc.stop()
  }
}
