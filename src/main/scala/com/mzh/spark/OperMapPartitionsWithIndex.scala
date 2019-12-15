package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperMapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)


    val indexRDD: RDD[(Int, Int)] = listRDD.mapPartitionsWithIndex {
      case (index, datas) =>
        datas.map((_, index))
    }


    println(indexRDD.collect().mkString(" "))

    sc.stop()
  }
}
