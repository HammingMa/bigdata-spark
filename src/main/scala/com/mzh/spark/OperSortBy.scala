package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object OperSortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(3,4,1,7,2,6,5))

    //val sortByRDD: RDD[Int] = listRDD.sortBy(x => x)
    val sortByRDD: RDD[Int] = listRDD.sortBy(x => x,false)

    println(sortByRDD.collect().mkString(" "))
  }
}
