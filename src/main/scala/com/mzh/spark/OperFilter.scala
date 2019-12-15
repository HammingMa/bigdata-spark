package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperFilter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val filterRDD: RDD[Int] = listRDD.filter(_%2 == 0)

    println(filterRDD.collect().mkString(" "))

    sc.stop()
  }
}
