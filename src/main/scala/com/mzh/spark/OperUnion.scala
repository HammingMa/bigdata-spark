package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperUnion {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
    val listRDD2: RDD[Int] = sc.makeRDD(6 to 10)

    val unionRDD: RDD[Int] = listRDD1.union(listRDD2)


    println(unionRDD.collect().mkString(" "))

    sc.stop()
  }
}
