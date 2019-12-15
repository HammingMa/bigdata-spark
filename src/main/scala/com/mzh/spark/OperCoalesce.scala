package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperCoalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)

    println(listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(2)

    println(coalesceRDD.partitions.size)

    coalesceRDD.saveAsTextFile("output")
    //println(distinctRDD.collect().mkString(" "))

    sc.stop()
  }
}
