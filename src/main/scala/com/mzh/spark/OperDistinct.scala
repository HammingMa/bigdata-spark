package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperDistinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,5,9,6,2,5,1,6,9))

    //val distinctRDD: RDD[Int] = listRDD.distinct()
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    distinctRDD.saveAsTextFile("output")
    //println(distinctRDD.collect().mkString(" "))

    sc.stop()
  }
}
