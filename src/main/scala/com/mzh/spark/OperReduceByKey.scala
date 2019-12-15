package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("one","two","three","one","two","one"))

    val mapRDD: RDD[(String, Int)] = listRDD.map((_,1))

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    println(reduceByKeyRDD.collect().mkString(" "))


    sc.stop()
  }
}
