package com.mzh.spark


import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object ShareDataAcc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val accumulator: LongAccumulator = sc.longAccumulator

    listRDD.foreach{
      (x) =>{
        accumulator.add(x)
      }
    }

    println("sum = "+accumulator.value)

  }
}
