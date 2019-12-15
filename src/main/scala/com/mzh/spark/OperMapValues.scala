package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperMapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    val mapValuesRDD: RDD[(Int, String)] = listRDD.mapValues(_+"_")

    println(mapValuesRDD.collect().mkString(" "))

    sc.stop()
  }
}
