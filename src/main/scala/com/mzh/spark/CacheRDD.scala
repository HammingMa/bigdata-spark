package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Cache RDD")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("a","b","c","d","a"))

    val mapRDD: RDD[(String, Int)] = listRDD.map((_,1))

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    //reduceByKeyRDD.cache()
    reduceByKeyRDD.persist(StorageLevel.DISK_ONLY)

    val sortByKey: RDD[(String, Int)] = reduceByKeyRDD.sortByKey()

    println(sortByKey.collect().mkString(" "))

    println(sortByKey.toDebugString)

  }
}
