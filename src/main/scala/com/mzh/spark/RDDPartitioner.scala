package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Cache RDD")
    val sc: SparkContext = new SparkContext(conf)


    val listRDD: RDD[String] = sc.makeRDD(List("a","b","c","d","a"))

    val mapRDD: RDD[(String, Int)] = listRDD.map((_,1))

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)


    val sortByKey: RDD[(String, Int)] = reduceByKeyRDD.sortByKey()

    println(sortByKey.collect().mkString(" "))

    println(listRDD.partitioner)
    println(reduceByKeyRDD.partitioner)

  }
}
